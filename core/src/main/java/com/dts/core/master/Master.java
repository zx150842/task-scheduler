package com.dts.core.master;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.queue.TaskQueueContext;
import com.dts.core.registration.RegisterClient;
import com.dts.core.registration.RpcRegisterMessage;
import com.dts.core.registration.ZKNodeChangeListener;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.core.util.Tuple2;
import com.dts.core.worker.Worker;
import com.dts.rpc.*;
import com.dts.rpc.exception.DTSException;
import com.dts.rpc.netty.NettyRpcEndpointRef;
import com.dts.rpc.netty.NettyRpcEnv;
import com.dts.rpc.util.SerializerInstance;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
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
  private final Map<String, List<WorkerInfo>> _workerGroups = Maps.newHashMap();
  private final Map<String, WorkerInfo> _idToWorker = Maps.newHashMap();
  private final Map<RpcAddress, WorkerInfo> _addressToWorker = Maps.newHashMap();
  private final ReentrantLock workerLock = new ReentrantLock();

  private final long WORKER_TIMEOUT_MS;
  private final long SEND_TASK_INTERVAL_MS;

  public static final String SYSTEM_NAME = "dtsMaster";
  public static final String ENDPOINT_NAME = "Master";

  private final TaskQueueContext taskQueueContext;
  private final String masterUrl;
  private final WorkerScheduler workerScheduler;

  private RecoveryState state = RecoveryState.STANDBY;
  private ZooKeeperPersistenceEngine persistenceEngine;
  private ZooKeeperLeaderElectionAgent leaderElectionAgent;
  private ScheduledFuture syncWorkerTask;
  private ScheduledFuture sendTaskToWorkTask;
  private RegisterClient registerClient;

  public Master(RpcEnv rpcEnv, RpcAddress address, DTSConf conf, TaskQueueContext taskQueueContext) {
    super(rpcEnv);
    this.address = address;
    this.masterUrl = address.hostPort;
    this.conf = conf;
    this.WORKER_TIMEOUT_MS = conf.getLong("dts.worker.timeout", 60) * 1000;
    this.SEND_TASK_INTERVAL_MS = conf.getLong("dts.master.sendTaskMs", 100);
    this.taskQueueContext = taskQueueContext;
    this.workerScheduler = new WorkerScheduler(conf);

    this.syncZKWorkerThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("sync-zookeeper-workers-thread");
    this.sendTaskToWorkThread = ThreadUtil.newDaemonSingleThreadScheduledExecutor("master-schedule-task-thread");
  }

  @Override
  public void onStart() {
    syncWorkerTask = syncZKWorkerThread.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        self().send(new SyncZKWorkers());
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    sendTaskToWorkTask = sendTaskToWorkThread.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        for (String workerGroup : _workerGroups.keySet()) {
          schedule(workerGroup);
        }
      }
    }, 0, SEND_TASK_INTERVAL_MS, TimeUnit.MILLISECONDS);

    SerializerInstance serializer = new SerializerInstance(conf);
    this.persistenceEngine = new ZooKeeperPersistenceEngine(conf, serializer);
    this.leaderElectionAgent = new ZooKeeperLeaderElectionAgent(this, conf);
    taskQueueContext.start();

    // 向注册中心注册master节点
    registerClient = new RegisterClient(conf, new WorkerNodeChangeListener());
    try {
      ServiceInstance instance = ServiceInstance.builder()
        .address(address.host)
        .port(address.port)
        .name(SYSTEM_NAME)
        .build();
      registerClient.registerService(instance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
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
    persistenceEngine.close();
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
      Tuple2<List<WorkerInfo>, List<ClientInfo>> tuple = persistenceEngine.readPersistedData(rpcEnv);
      List<WorkerInfo> storedWorkers = tuple._1;
      List<ClientInfo> storedClients = tuple._2;
      if (storedWorkers != null && storedWorkers.size() > 0
        && storedClients != null && storedClients.size() > 0
        && taskQueueContext.executingTaskQueue() != null) {
        state = RecoveryState.RECOVERING;
      } else {
        state = RecoveryState.ALIVE;
      }
      logger.info("I have been elected leader! New state: " + state);
      if (state == RecoveryState.RECOVERING) {
        recovery();
      }
    }

    else if (o instanceof RevokedLeadership) {
      logger.error("Leadership has been revoked -- master shutting down.");
      System.exit(0);
    }

    else if (o instanceof WorkerLastestState) {
      WorkerLastestState msg = (WorkerLastestState)o;
      if (_idToWorker.containsKey(msg.workerId)) {
        WorkerInfo workerInfo = _idToWorker.get(msg.workerId);
        workerInfo.setCoresUsed(msg.coreUsed);
        workerInfo.setMemoryUsed(msg.memoryUsed);
      } else {
        logger.warn("Worker state from unknown worker: " + msg.workerId);
      }
    }

    else if (o instanceof SyncZKWorkers) {
      syncZKWorkers();
    }

    else if (o instanceof  LaunchedTask) {
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
    if (content instanceof RegisterWorker) {
//      RegisterWorker msg = (RegisterWorker)content;
//      if (state == RecoveryState.STANDBY) {
//        context.reply(new MasterInStandby());
//      } else if (idToWorker.containsKey(msg.workerId)) {
//        context.reply(new RegisterWorkerFailed("Duplicate worker ID"));
//      } else {
//        WorkerInfo workerInfo = new WorkerInfo(msg.workerId, msg.cores, msg.memory,
//          msg.groupId, msg.worker);
//        if (registerWorker(workerInfo)) {
//          persistenceEngine.addWorker(workerInfo);
//          workerGroups.add(workerInfo.groupId);
//          context.reply(new RegisteredWorker(self()));
//          schedule(msg.groupId);
//        } else {
//          RpcAddress workerAddress = workerInfo.endpoint.address();
//          logger.warn("Worker registration failed. Attempted to re-register worker at same address: " + workerAddress);
//          context.reply(new RegisterWorkerFailed("Attempted to re-register worker at same address: " + workerAddress));
//        }
//      }
    }

    else if (content instanceof KillRunningTask) {
      KillRunningTask msg = (KillRunningTask)content;
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(msg.task.getWorkerGroup());
      Future future = worker.endpoint.ask(new KillRunningTask(msg.task));
      try {
        Object result = future.get(WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        context.reply(new KilledTask(msg.task, (String)result));
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace(); // TODO add kill failure
      } catch (TimeoutException e) {
        e.printStackTrace();
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

  private void recovery() {
    reregisterWorkers();
    if (state != RecoveryState.RECOVERING) {
      return;
    }

    state = RecoveryState.ALIVE;
    for (String workerGroup : _workerGroups.keySet()) {
      schedule(workerGroup);
    }
    logger.info("Recovery complete - resuming operations!");
  }

  private void schedule(String workerGroup) {
    if (state != RecoveryState.ALIVE) {
      return;
    }
    TriggeredTaskInfo task = taskQueueContext.get2LaunchingTask(workerGroup);
    if (task != null) {
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(workerGroup);
      // TODO send task to worker
      worker.endpoint.send(new LaunchTask(task));
    }
  }

  private void reregisterWorkers() {
    _workerGroups.clear();
    _idToWorker.clear();
    _addressToWorker.clear();

    Set<String> groups = registerClient.getAllServiceName();
    for (String workerGroup : groups) {
      List<RpcRegisterMessage> messages = registerClient.getByServiceName(workerGroup);
      if (messages == null || messages.isEmpty()) {
        logger.warn("WorkerGroup {} has no valid worker node", workerGroup);
        continue;
      }
      List<WorkerInfo> workerInfos = Lists.newArrayList();
      for (RpcRegisterMessage message : messages) {
        String workerId = message.detail.getWorkerId();
        RpcEndpointRef workerRef = new NettyRpcEndpointRef(conf, new RpcEndpointAddress(message.address, Worker.ENDPOINT_NAME),
          (NettyRpcEnv) rpcEnv);
        WorkerInfo workerInfo = new WorkerInfo(workerId, message.detail.getWorkerGroup(), workerRef);
        WorkerInfo oldWorkerInfo = _idToWorker.putIfAbsent(workerId, workerInfo);
        if (oldWorkerInfo != null) {
          logger.warn("Worker {} has already registered, ignore worker [address={}]",
            workerId, message.address);
          continue;
        }
        workerInfos.add(workerInfo);
        _addressToWorker.put(message.address, workerInfo);
      }
      _workerGroups.put(workerGroup, workerInfos);
    }
  }

  private void removeWorker(WorkerInfo worker) {
    logger.info("Removing worker {} on {}:{}", worker.id, worker.host, worker.port);
    worker.setState(WorkerState.DEAD);
    _idToWorker.remove(worker.id);
    _addressToWorker.remove(worker.endpoint.address());
  }

  private void syncZKWorkers() {
    Set<String> groups = registerClient.getAllServiceName();
    try {
      // 尝试获取写锁，如果获取不到说明其他线程在刷新缓存，直接返回
      if (workerLock.tryLock()) {
        // 删除不存在的worker group
        for (String workerGroup : _workerGroups.keySet()) {
          if (!groups.contains(workerGroup)) {
            unregisterWorkerGroup(workerGroup);
          }
        }
        for (String workerGroup : groups) {
          List<RpcRegisterMessage> messages = registerClient.getByServiceName(workerGroup);
          if (messages == null || messages.isEmpty()) {
            unregisterWorkerGroup(workerGroup);
          }
          syncWorkerGroup(workerGroup, messages);
        }
      }
    } finally {
      if (workerLock.isHeldByCurrentThread()) {
        workerLock.unlock();
      }
    }
  }

  // 这个类要首先获取锁
  private void unregisterWorkerGroup(String workerGroup) {
    _workerGroups.remove(workerGroup);
    for (WorkerInfo workerInfo : _idToWorker.values()) {
      if (workerGroup.equals(workerInfo.groupId)) {
        _idToWorker.remove(workerInfo.id);
      }
    }
    for (WorkerInfo workerInfo : _addressToWorker.values()) {
      if (workerGroup.equals(workerInfo.groupId)) {
        _addressToWorker.remove(workerInfo.endpoint.address());
      }
    }
  }

  // 这个类要首先获取锁
  private void syncWorkerGroup(String workerGroup, List<RpcRegisterMessage> messages) {
    Map<String, WorkerInfo> toUpdateWorkers = Maps.newHashMap();
    Set<String> unModifyWorkerIds = Sets.newHashSet();
    for (RpcRegisterMessage message : messages) {
      String workerId = message.detail.getWorkerId();
      if (_idToWorker.containsKey(workerId) && _addressToWorker.containsKey(message.address)) {
        unModifyWorkerIds.add(workerId);
      } else {
        RpcEndpointRef workerRef = new NettyRpcEndpointRef(conf, new RpcEndpointAddress(message.address, Worker.ENDPOINT_NAME),
          (NettyRpcEnv) rpcEnv);
        WorkerInfo workerInfo = new WorkerInfo(workerId, message.detail.getWorkerGroup(), workerRef);
        toUpdateWorkers.put(workerId, workerInfo);
        _idToWorker.put(workerId, workerInfo);
        _addressToWorker.put(message.address, workerInfo);
      }
    }
    List<WorkerInfo> newWorkerInfos = Lists.newArrayList();
    for (WorkerInfo workerInfo : _workerGroups.get(workerGroup)) {
      if (unModifyWorkerIds.contains(workerInfo.id)) {
        newWorkerInfos.add(workerInfo);
      } else if (toUpdateWorkers.containsKey(workerInfo.id)) {
        newWorkerInfos.add(toUpdateWorkers.get(workerInfo.id));
      } else {
        // remove this workerInfo
      }
    }
    _workerGroups.put(workerGroup, newWorkerInfos);
  }

  class WorkerNodeChangeListener implements ZKNodeChangeListener {

    @Override public void onChange(String workerGroup, List<RpcRegisterMessage> messages) {
      if (state == RecoveryState.STANDBY) {
        return;
      }
      try {
        // 尝试获取写锁，如果获取不到说明其他线程在刷新缓存，直接返回
        if (workerLock.tryLock()) {
          if (messages == null || messages.isEmpty()) {
            unregisterWorkerGroup(workerGroup);
          }
          syncWorkerGroup(workerGroup, messages);
        }
      } finally {
        if (workerLock.isHeldByCurrentThread()) {
          workerLock.unlock();
        }
      }
    }
  }

  public static void main(String[] args) {
    Preconditions.checkArgument(args == null || args.length != 1,
      "Master must start with property file path param.");
    String propertyFilePath = args[0];
    DTSConf conf = DTSConfUtil.readFile(propertyFilePath);
    RpcEnv rpcEnv = launchMaster(conf);
    rpcEnv.awaitTermination();
  }

  public static RpcEnv launchMaster(DTSConf conf) {
    RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, AddressUtil.getLocalHost(), 0, conf, false);
    TaskQueueContext context = new TaskQueueContext(conf);
    rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address(), conf, context));
    return rpcEnv;
  }
}
