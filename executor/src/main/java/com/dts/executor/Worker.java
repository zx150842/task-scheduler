package com.dts.executor;

import com.dts.core.DeployMessages;
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
import com.dts.core.rpc.RpcCallContext;
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
import java.util.concurrent.*;

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

  private final String WORKER_ID;
  private final String WORKER_GROUP_ID;
  private final int THREAD_NUM;
  private final int HEARTBEAT_INTERVAL_MS;
  private final int MASTER_TIMEOUT_MS;

  private final TaskMethodWrapper tw;

  private final RpcEnv rpcEnv;
  private final String host;
  private final int port;

  private final TaskDispatcher taskDispatcher;

  private final ExecutorService reportToMasterThread;

  private RpcEndpointRef master;
  private RegisterClient registerClient;
  private ServiceInstance<WorkerNodeDetail> instance;

  private final LinkedBlockingQueue<Object> reportMessageQueue = Queues.newLinkedBlockingQueue();
  private final ScheduledExecutorService sendHeartBeatThread;
  private ScheduledFuture sendHeartBeatTask;

  public Worker(RpcEnv rpcEnv, TaskMethodWrapper tw, DTSConf conf) {
    super(rpcEnv);
    this.conf = conf;

    this.rpcEnv = rpcEnv;
    this.host = rpcEnv.address().host;
    this.port = rpcEnv.address().port;
    this.tw = tw;

    this.WORKER_GROUP_ID = conf.get("dts.worker.groupId");
    this.THREAD_NUM = conf.getInt("dts.worker.threadNum", 10);
    this.WORKER_ID = AddressUtil.getLocalHost() + "-" + WORKER_GROUP_ID;
    this.HEARTBEAT_INTERVAL_MS = conf.getTransportConf("rpc").connectionTimeoutMs() / 4;
    this.MASTER_TIMEOUT_MS = conf.getInt("dts.master.timeoutMs", 500);

    this.sendHeartBeatThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("worker-send-heartbeat");
    this.taskDispatcher = new TaskDispatcher(conf, this, tw);
    this.reportToMasterThread = ThreadUtil.newDaemonSingleThreadExecutor("worker-reportToMaster");
    this.reportToMasterThread.submit(new ReportLoop());
  }

  @Override
  public void onStart() {
    logger.info("Starting worker {}:{}", host, port);
    try {
      registerClient = new RegisterClient(conf, new MasterNodeChangeListener());
      registerClient.start();
      instance = ServiceInstance.<WorkerNodeDetail>builder()
        .address(host)
        .port(port)
        .name(RegisterServiceName.WORKER)
        .payload(new WorkerNodeDetail(WORKER_ID, WORKER_GROUP_ID, THREAD_NUM, tw.taskMethodDetails))
        .build();
      registerClient.registerService(instance);
      List<RpcRegisterMessage> messages = registerClient.getByServiceName(RegisterServiceName.MASTER);
      refreshMaster(messages);

      sendHeartBeatTask = sendHeartBeatThread.scheduleAtFixedRate(new Runnable() {
        @Override public void run() {
          if (master != null) {
            logger.info("send heart to master {}", master.address());
            master.ask(new Heartbeat());
          }
        }
      }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    // TODO add metrics
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public boolean addToReportQueue(Object message) {
    return reportMessageQueue.offer(message);
  }

  @Override
  public void receive(Object o, RpcAddress senderAddress) {
    syncMaster(senderAddress);
    if (o instanceof LaunchTask) {
      LaunchTask msg = (LaunchTask)o;
      List<Object> params = deserializeTaskParams(msg.task.getTaskName(), msg.task.getParams());
      TaskWrapper tw = new TaskWrapper(msg.task, params);
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
      master.send(new KilledTask(msg.task, message));
    }
  }

  @Override
  public void receiveAndReply(Object o, RpcCallContext context) {
    if (o instanceof Heartbeat) {
      // do nothing
    }
  }

  @Override
  public void onDisconnected(RpcAddress remoteAddress) {
    if (master.address().equals(remoteAddress)) {
      logger.info("{} Disassociated", remoteAddress);
      logger.error("Connection to master failed! Waiting for master to reconnect");
      master = null;
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
    if (sendHeartBeatTask != null) {
      sendHeartBeatTask.cancel(true);
    }
    sendHeartBeatThread.shutdownNow();
  }

  private void syncMaster(RpcAddress address) {
    if (master == null || !master.address().equals(address)) {
      master = new NettyRpcEndpointRef(conf, new RpcEndpointAddress(address,
          EndpointNames.MASTER_ENDPOINT), (NettyRpcEnv) rpcEnv);
    }
  }

  private void refreshMaster(List<RpcRegisterMessage> messages) {
    for (RpcRegisterMessage message : messages) {
      try {
        RpcEndpointAddress address = new RpcEndpointAddress(message.address, EndpointNames.MASTER_ENDPOINT);
        RpcEndpointRef masterRef = new NettyRpcEndpointRef(conf, address, (NettyRpcEnv) rpcEnv);
        Future<Boolean> future = masterRef.ask(new AskMaster());
        Boolean isLeader = future.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (isLeader != null && isLeader) {
          master = masterRef;
          break;
        }
      } catch (Exception e) {
        logger.error(Throwables.getStackTraceAsString(e));
        continue;
      }
    }
  }

  class ReportLoop implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          Object message = reportMessageQueue.take();
          master.send(message);
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
