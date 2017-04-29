package com.dts.scheduler;

import com.dts.core.metrics.MetricsSystem;
import com.dts.core.registration.*;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.dts.core.EndpointNames;
import com.dts.core.DTSConf;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.RpcCallContext;
import com.dts.core.rpc.RpcEndpoint;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.exception.DTSException;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.scheduler.queue.TaskQueueContext;

import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.dts.core.DeployMessages.*;

/**
 * 调度器实现类，主要实现注册调度器，被选为leader节点的调度器下发任务到worker执行，
 * 并将执行结果保存
 *
 * @author zhangxin
 */
public class Master extends RpcEndpoint {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final RpcAddress address;
  private final ScheduledExecutorService syncZKWorkerThread;
  private final ExecutorService sendTaskToWorkThread;

  private final DTSConf conf;
  private Map<String, List<WorkerInfo>> _workerGroups = Maps.newHashMap();
  private Map<String, WorkerInfo> _idToWorker = Maps.newHashMap();
  private Map<RpcAddress, WorkerInfo> _addressToWorker = Maps.newHashMap();
  private final ReentrantLock workerLock = new ReentrantLock();

  private Map<String, RpcRegisterMessage> _idToWorkerDetails = Maps.newHashMap();

  private final long SYNC_WORKER_SEC;
  private final long WORKER_TIMEOUT_MS;
  private final int TASK_MAX_RETRY;

  public static final String SYSTEM_NAME = "dtsMaster";

  public final TaskQueueContext taskQueueContext;
  private final WorkerScheduler workerScheduler;

  public RecoveryState state = RecoveryState.STANDBY;
  private ZooKeeperLeaderElectionAgent leaderElectionAgent;
  private ScheduledFuture syncWorkerTask;
  private RegisterClient registerClient;
  private ServiceInstance instance;

  private final MetricsSystem metricsSystem;
  private final MasterSource masterSource;

  public Master(RpcEnv rpcEnv, RpcAddress address, DTSConf conf) {
    super(rpcEnv);
    this.address = address;
    this.conf = conf;
    this.SYNC_WORKER_SEC = conf.getLong("dts.master.syncWorkerSec", 60);
    this.WORKER_TIMEOUT_MS = conf.getLong("dts.worker.timeoutMs", 1000);
    this.TASK_MAX_RETRY = conf.getInt("dts.task.maxRetries", 3);
    this.metricsSystem = MetricsSystem.createMetricsSystem(conf);
    this.masterSource = new MasterSource(this);
    this.taskQueueContext = new TaskQueueContext(conf);
    this.workerScheduler = new WorkerScheduler(this);

    this.syncZKWorkerThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("sync-zookeeper-workers-thread");
    this.sendTaskToWorkThread = ThreadUtil.newDaemonSingleThreadExecutor("master-schedule-task-thread");
  }

  @Override
  public void onStart() {
    this.leaderElectionAgent = new ZooKeeperLeaderElectionAgent(this, conf);
    // 向注册中心注册master节点
    registerClient = new RegisterClient(conf, new WorkerNodeChangeListener());
    registerClient.start();
    try {
      instance = ServiceInstance.builder()
          .address(address.host)
          .port(address.port)
          .name(RegisterServiceName.MASTER)
          .build();
      registerClient.registerService(instance);

      metricsSystem.registerSource(masterSource);
      metricsSystem.start();
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  private void start() {
    if (state != RecoveryState.STANDBY) {
      return;
    }
    reregisterWorkers();
    syncWorkerTask = syncZKWorkerThread.scheduleAtFixedRate(
      () -> self().send(new SyncZKWorkers()), 0, SYNC_WORKER_SEC, TimeUnit.SECONDS);

    sendTaskToWorkThread.submit(() -> schedule());

    taskQueueContext.start();

    state = RecoveryState.ALIVE;
    logger.info("Start complete - resuming operations!");
  }

  @Override
  public void onStop() {
    if (syncWorkerTask != null) {
      syncWorkerTask.cancel(true);
    }
    if (registerClient != null) {
      if (instance != null) {
        registerClient.unregisterService(instance);
      }
      registerClient.close();
    }
    syncZKWorkerThread.shutdownNow();
    sendTaskToWorkThread.shutdownNow();
    leaderElectionAgent.stop();
    metricsSystem.stop();
  }

  public void resetToStandby() {
    if (state != RecoveryState.STANDBY) {
      state = RecoveryState.STANDBY;
    }
    if (syncWorkerTask != null) {
      syncWorkerTask.cancel(true);
    }
    if (syncZKWorkerThread != null) {
      syncZKWorkerThread.shutdownNow();
    }
    if (sendTaskToWorkThread != null) {
      sendTaskToWorkThread.shutdownNow();
    }

    taskQueueContext.stop();

    logger.info("Stop leader complete!");
  }

  public void electedLeader() {
    self().send(new ElectedLeader());
  }

  public void revokedLeadership() {
    self().send(new RevokedLeadership());
  }

  @Override
  public void receive(Object o, RpcAddress senderAddress) {
    if (o instanceof ElectedLeader) {
      start();
    }

    else if (o instanceof RevokedLeadership) {
      logger.error("Leadership has been revoked -- master stop.");
      resetToStandby();
    }

    else if (o instanceof SyncZKWorkers) {
      reregisterWorkers();
    }

    else if (o instanceof FinishTask) {
      FinishTask msg = (FinishTask)o;
      taskQueueContext.completeTask(msg.task.getSysId(), msg.message);
      if ("success".equals(msg.message)) {
        masterSource.successTaskMeter.mark();
      } else {
        masterSource.failTaskMeter.mark();
      }
    }
  }

  @Override
  public void receiveAndReply(Object o, RpcCallContext context) {
    if (o instanceof AskLeader) {
      if (state == RecoveryState.ALIVE) {
        context.reply(true);
      } else {
        context.reply(false);
      }
    }

    else if (o instanceof KillRunningTask) {
      KillRunningTask msg = (KillRunningTask)o;
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(msg.task.getWorkerGroup(), false);
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

    else if (o instanceof AskWorkers) {
      context.reply(new ReplyWorkers(Lists.newArrayList(_idToWorkerDetails.values())));
    }

    else if (o instanceof RefreshJobs) {
      boolean success = taskQueueContext.refreshJobs();
      int code = success ? SUCCESS : FAIL;
      String message = success ? "success" : "fail";
      context.reply(new RefreshedJobs(code, message));
    }

    else if (o instanceof RefreshWorkers) {
      reregisterWorkers();
      context.reply(new ReplyWorkers(Lists.newArrayList(_idToWorkerDetails.values())));
    }

    else if (o instanceof ManualTriggerJob) {
      logger.info("get manual job from client {}", o);
      ManualTriggerJob msg = (ManualTriggerJob)o;
      TriggeredTaskInfo task = taskQueueContext.manualTriggerJob(msg.jobConf, msg.runOnSeed);
      String sysId = task != null ? task.getSysId() : null;
      context.reply(new ManualTriggeredJob(sysId));
      logger.info("send manual job sysid to client {}", sysId);
    }

    else {
      context.sendFailure(new DTSException("Master not support receive msg type: " + o.getClass().getCanonicalName()));
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

  void schedule() {
    while (true) {
      try {
        TriggeredTaskInfo task = taskQueueContext.get2ExecutingTask();
        if (task != null) {
          logger.trace("To send to worker task: {}", task);
          WorkerInfo worker = workerScheduler.getLaunchTaskWorker(task.getWorkerGroup(), task.isRunOnSeed());
          if (worker != null) {
            logger.trace("Select worker {} to execute task {}", worker.id, task);
            task.setWorkerId(worker.id);
            taskQueueContext.executingTask(task);
            worker.endpoint.send(new LaunchTask(task));
            masterSource.sendTaskMeter.mark();
          } else {
            logger.info("No worker is valid, resume task {}", task);
            task.incrRetryCount();
            if (task.getRetryCount() > TASK_MAX_RETRY) {
              logger.error("Task {} has retry {} times, discard task", task, task.getRetryCount());
              taskQueueContext.removeOverRetryTask(task);
              masterSource.discardRetryTaskMeter.mark();
            } else {
              taskQueueContext.resumeExecutableTask(task);
              masterSource.resumeTaskMeter.mark();
            }
          }
        }
      } catch (InterruptedException e) {
        logger.error(Throwables.getStackTraceAsString(e));
      }
    }
  }

  private boolean reregisterWorkers() {
    masterSource.syncWorkerMeter.mark();
    List<RpcRegisterMessage> messages = registerClient.getByServiceName(RegisterServiceName.WORKER);
    try {
      if (workerLock.tryLock()) {
        Map<String, List<WorkerInfo>> workerGroups = Maps.newHashMap();
        Map<String, WorkerInfo> idToWorker = Maps.newHashMap();
        Map<RpcAddress, WorkerInfo> addressToWorker = Maps.newHashMap();
        Map<String, RpcRegisterMessage> idToWorkerDetails = Maps.newHashMap();
        if (messages == null || messages.isEmpty()) {
          logger.warn("Service name {} has no valid node", RegisterServiceName.WORKER);
          _workerGroups = workerGroups;
          _idToWorker = idToWorker;
          _addressToWorker = addressToWorker;
          _idToWorkerDetails = idToWorkerDetails;
          return false;
        }
        for (RpcRegisterMessage message : messages) {
          String workerId = message.detail.getWorkerId();
          RpcEndpointRef workerRef = new NettyRpcEndpointRef(conf, new RpcEndpointAddress(message.address, EndpointNames.WORKER_ENDPOINT),
              (NettyRpcEnv) rpcEnv);
          WorkerInfo workerInfo = new WorkerInfo(workerId, message.detail.getWorkerGroup(), message.detail.isSeed(), workerRef);
          idToWorker.put(workerId, workerInfo);
          addressToWorker.put(message.address, workerInfo);
          if (workerGroups.containsKey(workerInfo.groupId)) {
            workerGroups.get(workerInfo.groupId).add(workerInfo);
          } else {
            List<WorkerInfo> workerInfos = Lists.newArrayList(workerInfo);
            workerGroups.put(workerInfo.groupId, workerInfos);
          }
          idToWorkerDetails.put(message.detail.getWorkerId(), message);
        }
        _workerGroups = workerGroups;
        _idToWorker = idToWorker;
        _addressToWorker = addressToWorker;
        _idToWorkerDetails = idToWorkerDetails;
      }
      return true;
    } finally {
      resumeExecutingTaskToValidWork();
      if (workerLock.isHeldByCurrentThread()) {
        workerLock.unlock();
      }
    }
  }

  /**
   * 每次刷新worker后都检查一次正在运行的task所在的worker是否已经被移除，如果worker已经不存在，则
   * 将被调度到这个worker运行的task重新调度到其他worker执行
   */
  private void resumeExecutingTaskToValidWork() {
    Collection<TriggeredTaskInfo> executingTasks = taskQueueContext.executingTaskCache.values();
    for (TriggeredTaskInfo task : executingTasks) {
      String workerId = task.getWorkerId();
      String sysId = task.getSysId();
      String lastSysId = taskQueueContext.lastExecuteTaskSysIdCache.get(task.getTaskId());
      if (_idToWorker == null || !_idToWorker.containsKey(workerId)) {
        if (lastSysId != null && lastSysId.compareTo(sysId) <= 0) {
          logger.warn("Worker {} is invalid, resume this worker executing task {} to other worker",
            workerId, task);
          taskQueueContext.resumeExecutingTask(task);
        }
      }
    }
  }

  private void removeWorker(WorkerInfo worker) {
    if (worker == null) {
      return;
    }
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

  public static Master launchMaster(DTSConf conf) {
    int port = conf.getInt("dts.master.port", 0);
    RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, AddressUtil.getLocalHost(), port, conf, false);
    Master master = new Master(rpcEnv, rpcEnv.address(), conf);
    rpcEnv.setupEndpoint(EndpointNames.MASTER_ENDPOINT, master);
    return master;
  }
}
