package com.dts.executor;

import com.dts.core.metrics.MetricsSystem;
import com.dts.core.registration.*;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import com.dts.core.EndpointNames;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DataTypeUtil;
import com.dts.core.DTSConf;
import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.RpcEndpoint;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.exception.DTSException;
import com.dts.core.util.ThreadUtil;
import com.dts.executor.task.TaskMethodWrapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.dts.core.DeployMessages.*;

/**
 * 执行器实现类，主要实现向注册中心注册当前执行器，接收调度器下发的任务，
 * 执行并向调度器上报执行结果
 *
 * @author zhangxin
 */
public class Worker extends RpcEndpoint {
  private static final Logger logger = LoggerFactory.getLogger(Worker.class);
  private static final String SYSTEM_NAME = "dtsWorker";

  private final DTSConf conf;

  public final String WORKER_ID;
  public final String WORKER_GROUP_ID;
  public final int THREAD_NUM;
  public final int SYNC_MASTER_SEC;
  public final int MASTER_TIMEOUT_MS;

  private final TaskMethodWrapper tw;

  private final RpcEnv rpcEnv;
  private final String host;
  private final int port;

  private final TaskDispatcher taskDispatcher;

  private final ExecutorService reportToMasterThread;

  public Optional<RpcEndpointRef> master = Optional.empty();
  public final RpcEndpointRef UNKNOWN_MASTER;
  private static final ReentrantLock masterLock = new ReentrantLock();
  private RegisterClient registerClient;
  private ServiceInstance<NodeDetail> instance;

  private final LinkedBlockingQueue<TaskResult> reportMessageQueue = Queues.newLinkedBlockingQueue();
  private final ScheduledExecutorService syncMasterThread;
  private ScheduledFuture syncMasterTask;

  private final MetricsSystem metricsSystem;
  public final WorkerSource workerSource;

  private final boolean isSeed;

  public Worker(RpcEnv rpcEnv, TaskMethodWrapper tw, DTSConf conf) {
    super(rpcEnv);
    this.conf = conf;

    this.rpcEnv = rpcEnv;
    this.host = rpcEnv.address().host;
    this.port = rpcEnv.address().port;
    this.tw = tw;

    this.WORKER_GROUP_ID = conf.get("dts.worker.groupId");
    this.THREAD_NUM = conf.getInt("dts.worker.threads", 10);
    this.WORKER_ID = AddressUtil.getLocalHost() + "-" + WORKER_GROUP_ID;
    this.SYNC_MASTER_SEC = conf.getInt("dts.worker.syncMasterSec", 60);
    this.MASTER_TIMEOUT_MS = conf.getInt("dts.master.timeoutMs", 1000);
    this.UNKNOWN_MASTER = new NettyRpcEndpointRef(conf, new RpcEndpointAddress("unknown", -1, "unknown"), (NettyRpcEnv) rpcEnv);

    this.syncMasterThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("worker-syncMaster");
    this.taskDispatcher = new TaskDispatcher(conf, this, tw);
    this.reportToMasterThread = ThreadUtil.newDaemonSingleThreadExecutor("worker-reportToMaster");
    this.reportToMasterThread.submit(new ReportLoop());

    this.isSeed = conf.getBoolean("dts.worker.stage", false);

    this.metricsSystem = MetricsSystem.createMetricsSystem(conf);
    this.workerSource = new WorkerSource(this);
  }

  @Override
  public void onStart() {
    logger.info("Starting worker {}:{}", host, port);
    try {
      registerClient = new RegisterClient(conf, new MasterNodeChangeListener());
      registerClient.start();
      instance = ServiceInstance.<NodeDetail>builder()
        .address(host)
        .port(port)
        .name(RegisterServiceName.WORKER)
        .payload(new NodeDetail(WORKER_ID, WORKER_GROUP_ID, THREAD_NUM, tw.taskMethodDetails, isSeed))
        .build();
      registerClient.registerService(instance);
      syncMasterTask = syncMasterThread.scheduleAtFixedRate(() -> refreshMaster(), 0, SYNC_MASTER_SEC, TimeUnit.SECONDS);

      metricsSystem.registerSource(workerSource);
      metricsSystem.start();
      logger.info("Worker {} on {} started", WORKER_ID, rpcEnv.address());
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public boolean addToReportQueue(TaskResult message) {
    return reportMessageQueue.offer(message);
  }

  @Override
  public void receive(Object o, RpcAddress senderAddress) {
    workerSource.taskReceiveMeter.mark();
    resetMaster(senderAddress);
    if (o instanceof LaunchTask) {
      LaunchTask msg = (LaunchTask)o;
      List<Object> params = deserializeTaskParams(msg.task.getTaskName(), msg.task.getParams());
      TaskWrapper tw = new TaskWrapper(msg.task, params, workerSource.taskTotalTimer.time());
      try {
        if (!taskDispatcher.addTask(tw)) {
          logger.error("Failed to add task to worker task {} queue", msg.task);
        }
      } catch (Exception e) {
        logger.error(Throwables.getStackTraceAsString(e));
      }
    }

    else if (o instanceof KillRunningTask) {
      KillRunningTask msg = (KillRunningTask)o;
      // TODO kill task
      String message = null;
      //      if (taskDispatcher.stopTask(msg.task.getSysId())) {
      //        message = "success";
      //      } else {
      //        message = "Failed to stop task: " + msg.task;
      //      }
      reportMessageQueue.offer(new TaskResult(new KilledTask(msg.task, message), workerSource.taskTotalTimer.time()));
    }
  }

  @Override
  public void onDisconnected(RpcAddress remoteAddress) {
    logger.info("{} Disassociated", remoteAddress);
    if (remoteAddress.equals(master.orElse(UNKNOWN_MASTER).address())) {
      logger.info("Worker client to master: {} Disassociated", master.orElse(UNKNOWN_MASTER).address());
    }
  }

  @Override
  public void onStop() {
    if (registerClient != null) {
      if (instance != null) {
        registerClient.unregisterService(instance);
      }
      registerClient.close();
    }
    if (syncMasterTask != null) {
      syncMasterTask.cancel(true);
    }
    syncMasterThread.shutdownNow();
    metricsSystem.report();
    metricsSystem.stop();
  }

  private void resetMaster(RpcAddress address) {
    if (!master.orElse(UNKNOWN_MASTER).address().equals(address)) {
      try {
        masterLock.lock();
        RpcAddress masterAddress = master.orElse(UNKNOWN_MASTER).address();
        logger.warn("Receive message from sender: {}, current master: {}, change master to sender",
          address, masterAddress);
        master = Optional.of(new NettyRpcEndpointRef(conf, new RpcEndpointAddress(address, EndpointNames.MASTER_ENDPOINT), (NettyRpcEnv) rpcEnv));
      } finally {
        if (masterLock.isHeldByCurrentThread()) {
          masterLock.unlock();
        }
      }
    }
  }

  private void refreshMaster() {
    List<RpcRegisterMessage> messages = registerClient.getByServiceName(RegisterServiceName.MASTER);
    refreshMaster(messages);
  }

  private void refreshMaster(List<RpcRegisterMessage> messages) {
    workerSource.syncMasterMeter.mark();
    try {
      masterLock.lock();
      for (RpcRegisterMessage message : messages) {
        try {
          RpcEndpointAddress address = new RpcEndpointAddress(message.address, EndpointNames.MASTER_ENDPOINT);
          RpcEndpointRef masterRef = new NettyRpcEndpointRef(conf, address, (NettyRpcEnv) rpcEnv);
          Future<Boolean> future = masterRef.ask(new AskLeader());
          Boolean isLeader = future.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (isLeader != null && isLeader) {
            master = Optional.of(masterRef);
            break;
          }
        } catch (Exception e) {
          logger.error(Throwables.getStackTraceAsString(e));
          continue;
        }
      }
      if (!master.isPresent()) {
        logger.error("Cannot find master!");
      }
    } finally {
      if (masterLock.isHeldByCurrentThread()) {
        masterLock.unlock();
      }
    }
  }

  class ReportLoop implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          TaskResult message = reportMessageQueue.take();
          if (!master.isPresent()) {
            refreshMaster();
          }
          if (master.isPresent()) {
            master.get().send(message.task);
            message.taskContext.stop();
          }
        } catch (InterruptedException e) {
          // exit
        } catch (Exception e) {
          logger.error("Report message to master error.", e);
        }
      }
    }
  }

  class MasterNodeChangeListener implements ZKNodeChangeListener {

    @Override
    public void onChange(String serviceName, List<RpcRegisterMessage> messages) {
      refreshMaster(messages);
    }

    @Override
    public List<String> getListeningServiceNames() {
      return Lists.newArrayList(RegisterServiceName.MASTER);
    }
  }

  public static Worker launchWorker(
      String host, int port, TaskMethodWrapper tw, DTSConf conf) {
    RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, false);

    Worker worker = new Worker(rpcEnv, tw, conf);
    rpcEnv.setupEndpoint(EndpointNames.WORKER_ENDPOINT, worker);
    return worker;
  }

  private List<Object> deserializeTaskParams(String taskName, String params) {
    List<Object> paramValues = Lists.newArrayList();
    if (StringUtils.isBlank(params)) {
      return paramValues;
    }
    String[] values = params.split(",");
    Method method = tw.taskMethods.get(taskName);
    if (method == null) {
      throw new DTSException("Cannot find method of taskName: " + taskName);
    }
    if(method.getParameters().length != values.length) {
      throw new DTSException(String.format("TaskName %s, params length is not equal method param length", taskName));
    }
    for (int i = 0; i < method.getParameters().length; ++i) {
      Parameter param = method.getParameters()[i];
      paramValues.add(DataTypeUtil.convertToPrimitiveType(param.getType().getSimpleName(), StringUtils.trimToEmpty(values[i])));
    }
    return paramValues;
  }
}
