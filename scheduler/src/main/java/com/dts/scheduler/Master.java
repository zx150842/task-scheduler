package com.dts.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.dts.core.EndpointNames;
import com.dts.core.DTSConf;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.registration.RegisterClient;
import com.dts.core.registration.RegisterServiceName;
import com.dts.core.registration.RpcRegisterMessage;
import com.dts.core.registration.ZKNodeChangeListener;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.core.util.Tuple2;
import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.RpcCallContext;
import com.dts.core.rpc.RpcEndpoint;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.exception.DTSException;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.rpc.util.SerializerInstance;
import com.dts.scheduler.queue.TaskQueueContext;

import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class Master extends RpcEndpoint {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final RpcAddress address;
  private final ScheduledExecutorService syncZKWorkerThread;
  private final ScheduledExecutorService sendTaskToWorkThread;

  private final DTSConf conf;
  private Map<String, List<WorkerInfo>> _workerGroups = Maps.newHashMap();
  private Map<String, WorkerInfo> _idToWorker = Maps.newHashMap();
  private Map<RpcAddress, WorkerInfo> _addressToWorker = Maps.newHashMap();
  private final ReentrantLock workerLock = new ReentrantLock();

  private final long SYNC_WORKER_SEC;
  private final long SEND_TASK_INTERVAL_MS;
  private final long WORKER_TIMEOUT_MS;

  public static final String SYSTEM_NAME = "dtsMaster";

  private final TaskQueueContext taskQueueContext;
  private final WorkerScheduler workerScheduler;

  private RecoveryState state = RecoveryState.STANDBY;
  private ZooKeeperLeaderElectionAgent leaderElectionAgent;
  private ScheduledFuture syncWorkerTask;
  private ScheduledFuture sendTaskToWorkTask;
  private RegisterClient registerClient;

  public Master(RpcEnv rpcEnv, RpcAddress address, DTSConf conf, TaskQueueContext taskQueueContext) {
    super(rpcEnv);
    this.address = address;
    this.conf = conf;
    this.SYNC_WORKER_SEC = conf.getLong("dts.master.syncWorkerSec", 60);
    this.WORKER_TIMEOUT_MS = conf.getLong("dts.worker.timeoutMs", 60) * 1000;
    this.SEND_TASK_INTERVAL_MS = conf.getLong("dts.master.sendTaskMs", 100);
    this.taskQueueContext = taskQueueContext;
    this.workerScheduler = new WorkerScheduler(this);

    this.syncZKWorkerThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("sync-zookeeper-workers-thread");
    this.sendTaskToWorkThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("master-schedule-task-thread");
  }

  @Override
  public void onStart() {
    this.leaderElectionAgent = new ZooKeeperLeaderElectionAgent(this, conf);
    // 向注册中心注册master节点
    registerClient = new RegisterClient(conf, new WorkerNodeChangeListener());
    registerClient.start();
    try {
      ServiceInstance instance = ServiceInstance.builder()
          .address(address.host)
          .port(address.port)
          .name(RegisterServiceName.MASTER)
          .build();
      registerClient.registerService(instance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  private void start() {
    if (state != RecoveryState.STANDBY) {
      return;
    }

    syncWorkerTask = syncZKWorkerThread.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        self().send(new SyncZKWorkers());
      }
    }, 0, SYNC_WORKER_SEC, TimeUnit.SECONDS);

    sendTaskToWorkTask = sendTaskToWorkThread.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        for (String workerGroup : _workerGroups.keySet()) {
          schedule(workerGroup);
        }
      }
    }, 0, SEND_TASK_INTERVAL_MS, TimeUnit.MILLISECONDS);

    taskQueueContext.start();

    reregisterWorkers();

    state = RecoveryState.ALIVE;
    for (String workerGroup : _workerGroups.keySet()) {
      schedule(workerGroup);
    }
    logger.info("Start complete - resuming operations!");
  }

  @Override
  public void onStop() {
    if (syncWorkerTask != null) {
      syncWorkerTask.cancel(true);
    }
    if (sendTaskToWorkTask != null) {
      sendTaskToWorkTask.cancel(true);
    }
    if (registerClient != null) {
      registerClient.close();
    }
    syncZKWorkerThread.shutdownNow();
    sendTaskToWorkThread.shutdownNow();
    leaderElectionAgent.stop();
  }

  public void electedLeader() {
    self().send(new ElectedLeader());
  }

  public void revokedLeadership() {
    self().send(new RevokedLeadership());
  }

  @Override
  public void receive(Object o) {
    if (o instanceof ElectedLeader) {
      start();
    }

    else if (o instanceof RevokedLeadership) {
      logger.error("Leadership has been revoked -- master shutting down.");
      System.exit(0);
    }

    else if (o instanceof SyncZKWorkers) {
      reregisterWorkers();
    }

    else if (o instanceof LaunchedTask) {
      LaunchedTask msg = (LaunchedTask)o;
      taskQueueContext.launchedTask(msg.task);
      // TODO check return
    }

    else if (o instanceof ExecutingTask) {
      ExecutingTask msg = (ExecutingTask)o;
      taskQueueContext.executingTask(msg.task);
      // TODO check return
    }

    else if (o instanceof FinishTask) {
      FinishTask msg = (FinishTask)o;
      taskQueueContext.completeTask(msg.task);
    }

    else if (o instanceof ManualTriggerJob) {
      ManualTriggerJob msg = (ManualTriggerJob)o;
      taskQueueContext.manualTriggerJob(msg.jobConf);
    }
  }

  @Override
  public void receiveAndReply(Object content, RpcCallContext context) {
    if (content instanceof KillRunningTask) {
      KillRunningTask msg = (KillRunningTask)content;
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(msg.task.getWorkerGroup());
      if (worker != null) {
        Future future = worker.endpoint.ask(new KillRunningTask(msg.task));
        try {
          Object result = future.get(WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          context.reply(new KilledTask(msg.task, (String) result));
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace(); // TODO add kill failure
        } catch (TimeoutException e) {
          e.printStackTrace();
        }
      }
    }

    else {
      context.sendFailure(new DTSException("Master not support receive msg type: " + content.getClass().getCanonicalName()));
    }
  }

  @Override
  public void onDisconnected(RpcAddress address) {
    WorkerInfo workerInfo = _addressToWorker.get(address);
    removeWorker(workerInfo);
  }

  public Map<String, List<WorkerInfo>> workerGroups() {
    return _workerGroups;
  }

  private void schedule(String workerGroup) {
    if (state != RecoveryState.ALIVE) {
      return;
    }
    TriggeredTaskInfo task = taskQueueContext.get2LaunchingTask(workerGroup);
    if (task != null) {
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(workerGroup);
      if (worker != null) {
        worker.endpoint.send(new LaunchTask(task));
      }
    }
  }

  private boolean reregisterWorkers() {
    List<RpcRegisterMessage> messages = registerClient.getByServiceName(RegisterServiceName.WORKER);
    if (messages == null || messages.isEmpty()) {
      logger.warn("Service name {} has no valid node", RegisterServiceName.WORKER);
      return false;
    }
    try {
      if (workerLock.tryLock()) {
        Map<String, List<WorkerInfo>> workerGroups = Maps.newHashMap();
        Map<String, WorkerInfo> idToWorker = Maps.newHashMap();
        Map<RpcAddress, WorkerInfo> addressToWorker = Maps.newHashMap();
        for (RpcRegisterMessage message : messages) {
          String workerId = message.detail.getWorkerId();
          RpcEndpointRef workerRef = new NettyRpcEndpointRef(conf, new RpcEndpointAddress(message.address, EndpointNames.WORKER_ENDPOINT),
              (NettyRpcEnv) rpcEnv);
          WorkerInfo workerInfo = new WorkerInfo(workerId, message.detail.getWorkerGroup(), workerRef);
          if (_idToWorker.containsKey(workerId)) {
            logger.warn("Worker {} has already registered, ignore worker [address={}]",
                workerId, message.address);
            continue;
          }
          idToWorker.put(workerId, workerInfo);
          addressToWorker.put(message.address, workerInfo);
          if (workerGroups.containsKey(workerInfo.groupId)) {
            workerGroups.get(workerInfo.groupId).add(workerInfo);
          } else {
            List<WorkerInfo> workerInfos = Lists.newArrayList(workerInfo);
            workerGroups.put(workerInfo.groupId, workerInfos);
          }
        }
        _workerGroups = workerGroups;
        _idToWorker = idToWorker;
        _addressToWorker = addressToWorker;
      }
      return true;
    } finally {
      if (workerLock.isHeldByCurrentThread()) {
        workerLock.unlock();
      }
    }
  }

  private void removeWorker(WorkerInfo worker) {
    logger.info("Removing worker {} on {}:{}", worker.id, worker.host, worker.port);
    worker.setState(WorkerState.DEAD);
    try {
      if (workerLock.tryLock()) {
        _idToWorker.remove(worker.id);
        _addressToWorker.remove(worker.endpoint.address());
        List<WorkerInfo> workers = _workerGroups.get(worker.groupId);
        WorkerInfo toRemoveWorker = null;
        for (WorkerInfo workerInfo : workers) {
          if (worker.id.equals(workerInfo.id)) {
            toRemoveWorker = workerInfo;
            break;
          }
        }
        if (toRemoveWorker != null) {
          workers.remove(toRemoveWorker);
          if (workers == null || workers.isEmpty()) {
            _workerGroups.remove(worker.groupId);
          }
        }
      }
    } finally {
      if (workerLock.isHeldByCurrentThread()) {
        workerLock.unlock();
      }
    }
  }

  class WorkerNodeChangeListener implements ZKNodeChangeListener {

    @Override public void onChange(String serviceName, List<RpcRegisterMessage> messages) {
      if (state == RecoveryState.STANDBY) {
        return;
      }
      reregisterWorkers();
    }

    @Override
    public List<String> getListeningServiceNames() {
      return Lists.newArrayList(RegisterServiceName.WORKER);
    }
  }

  public static void main(String[] args) {
    Preconditions.checkArgument(args == null || args.length != 1,
      "Master must start with property file path param.");
    String propertyFilePath = args[0];
    DTSConf conf = DTSConfUtil.readFile(propertyFilePath);
    Master master = launchMaster(conf);
    master.rpcEnv.awaitTermination();
  }

  public static Master launchMaster(DTSConf conf) {
    RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, AddressUtil.getLocalHost(), 0, conf, false);
    TaskQueueContext context = new TaskQueueContext(conf);
    Master master = new Master(rpcEnv, rpcEnv.address(), conf, context);
    rpcEnv.setupEndpoint(EndpointNames.MASTER_ENDPOINT, master);
    return master;
  }
}
