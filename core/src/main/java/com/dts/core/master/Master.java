package com.dts.core.master;

import com.dts.core.TaskConf;
import com.dts.core.TaskGroup;
import com.dts.core.TaskInfo;
import com.dts.core.queue.TaskQueueContext;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.util.Tuple2;
import com.dts.rpc.*;
import com.dts.rpc.netty.NettyRpcEnv;
import com.dts.rpc.util.SerializerInstance;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class Master extends RpcEndpoint {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final RpcAddress address;
  private final ScheduledThreadPoolExecutor forwardMessageThread;

  private final DTSConf conf;
  private final Set<WorkerInfo> workers = Sets.newHashSet();
  private final Map<String, WorkerInfo> idToWorker = Maps.newHashMap();
  private final Map<RpcAddress, WorkerInfo> addressToWorker = Maps.newHashMap();
  private final Set<String> workerGroups = Sets.newHashSet();

  private final Set<ClientInfo> clients = Sets.newHashSet();
  private final Map<String, ClientInfo> idToClient = Maps.newHashMap();
  private final Map<RpcAddress, ClientInfo> addressToClient = Maps.newHashMap();

  private final long WORKER_TIMEOUT_MS;
  private final long REAPER_ITERATIONS;

  public static final String ENDPOINT_NAME = "Master";

  private final TaskQueueContext taskQueueContext;
  private final String masterUrl;
  private final WorkerScheduler workerScheduler;

  private RecoveryState state = RecoveryState.STANDBY;
  private ZooKeeperPersistenceEngine persistenceEngine;
  private ZooKeeperLeaderElectionAgent leaderElectionAgent;
  private ScheduledFuture recoveryCompletionTask;
  private ScheduledFuture checkForWorkerTimeOutTask;

  public Master(NettyRpcEnv rpcEnv, RpcAddress address, DTSConf conf, TaskQueueContext taskQueueContext) {
    super(rpcEnv);
    this.address = address;
    this.masterUrl = address.getHostPort();
    this.conf = conf;
    this.WORKER_TIMEOUT_MS = conf.getLong("dts.worker.timeout", 60) * 1000;
    this.REAPER_ITERATIONS = conf.getInt("dts.dead.worker.persistence", 15);
    this.taskQueueContext = taskQueueContext;
    this.workerScheduler = new WorkerScheduler(conf);

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("master-forward-message-thread").build();
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
    executor.setRemoveOnCancelPolicy(true);
    this.forwardMessageThread = executor;
  }

  @Override
  public void onStart() {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        self().send(new CheckForWorkerTimeOut());
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    SerializerInstance serializer = new SerializerInstance(conf);
    this.persistenceEngine = new ZooKeeperPersistenceEngine(conf, serializer);
    this.leaderElectionAgent = new ZooKeeperLeaderElectionAgent(this, conf);
    taskQueueContext.start();
  }

  @Override
  public void onStop() {
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true);
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true);
    }
    forwardMessageThread.shutdownNow();
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
      Tuple2<WorkerInfo[], ClientInfo[]> tuple = persistenceEngine.readPersistedData(rpcEnv);
      WorkerInfo[] storedWorkers = tuple._1;
      ClientInfo[] storedClients = tuple._2;
      if (storedWorkers != null && storedClients != null && taskQueueContext.executingTaskQueue() != null) {
        state = RecoveryState.RECOVERING;
      } else {
        state = RecoveryState.ALIVE;
      }
      logger.info("I have been elected leader! New state: " + state);
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedWorkers, storedClients);
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable() {
          @Override public void run() {
            self().send(new CompleteRecovery());
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      }
    }

    else if (o instanceof CompleteRecovery) {
      completeRecovery();
    }

    else if (o instanceof RevokedLeadership) {
      logger.error("Leadership has been revoked -- master shutting down.");
      System.exit(0);
    }

    else if (o instanceof Heartbeat) {
      Heartbeat msg = (Heartbeat)o;
      if (idToWorker.containsKey(msg.workerId)) {
        WorkerInfo workerInfo = idToWorker.get(msg.workerId);
        workerInfo.setLastHeartbeat(System.currentTimeMillis());
      } else {
        boolean findWorkerId = false;
        for (WorkerInfo workerInfo : workers) {
          if (workerInfo.id.equals(msg.workerId)) {
            findWorkerId = true;
            logger.warn("Got heartbeat from unregistered worker {}. "
              + "Asking it to re-register", msg.workerId);
            msg.worker.send(new ReconnectWorker(masterUrl));
            break;
          }
        }
        if (!findWorkerId) {
          logger.warn("Got heartbeat from unregistered worker {}. "
            + "This worker was never registered, so ignoring the heartbeat", msg.workerId);
        }
      }
    }

    else if (o instanceof WorkerLastestState) {
      WorkerLastestState msg = (WorkerLastestState)o;
      if (idToWorker.containsKey(msg.workerId)) {
        WorkerInfo workerInfo = idToWorker.get(msg.workerId);
        workerInfo.setCoresUsed(msg.coreUsed);
        workerInfo.setMemoryUsed(msg.memoryUsed);
      } else {
        logger.warn("Worker state from unknown worker: " + msg.workerId);
      }
    }

    else if (o instanceof CheckForWorkerTimeOut) {
      timeOutDeadWorkers();
    }

    else if (o instanceof MasterChangeAckFromClient) {
      MasterChangeAckFromClient msg = (MasterChangeAckFromClient)o;
      ClientInfo client = idToClient.get(msg.clientId);
      if (client != null) {
        logger.info("Client has been re-registered: " + msg.clientId);
        client.setState(ClientState.ALIVE);
      } else {
        logger.warn("Master change ack from unknown client: " + msg.clientId);
      }
    }

    else if (o instanceof MasterChangeAckFromWorker) {
      MasterChangeAckFromWorker msg = (MasterChangeAckFromWorker)o;
      WorkerInfo worker = idToWorker.get(msg.workerId);
      if (worker != null) {
        logger.info("Worker has been re-registered: " + msg.workerId);
        worker.setState(WorkerState.ALIVE);
        worker.setCoresUsed(msg.coreUsed);
        worker.setMemoryUsed(msg.memoryUsed);
      } else {
        logger.warn("Scheduler state from unknown worker: " + msg.workerId);
      }
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
      TaskInfo nextTask = taskQueueContext.completeTask(msg.task);
      if (nextTask != null) {
        taskQueueContext.addNextTaskToExecutableQueue(nextTask);
      }
    }

    else if (o instanceof ManualTriggerTask) {
      ManualTriggerTask msg = (ManualTriggerTask)o;
      taskQueueContext.triggerTask(msg.taskId);
    }

    else if (o instanceof ManualTriggerTaskGroup) {
      ManualTriggerTaskGroup msg = (ManualTriggerTaskGroup)o;
      taskQueueContext.triggerTaskGroup(msg.taskGroupId);
    }
  }

  @Override
  public void receiveAndReply(Object content, RpcCallContext context) {
    if (content instanceof RegisterWorker) {
      RegisterWorker msg = (RegisterWorker)content;
      if (state == RecoveryState.STANDBY) {
        context.reply(new MasterInStandby());
      } else if (idToWorker.containsKey(msg.workerId)) {
        context.reply(new RegisterWorkerFailed("Duplicate worker ID"));
      } else {
        WorkerInfo workerInfo = new WorkerInfo(msg.workerId, msg.host, msg.port, msg.cores, msg.memory,
          msg.worker, msg.groupId);
        if (registerWorker(workerInfo)) {
          persistenceEngine.addWorker(workerInfo);
          workerGroups.add(workerInfo.groupId);
          context.reply(new RegisteredWorker(self()));
          schedule(msg.groupId);
        } else {
          RpcAddress workerAddress = workerInfo.endpoint.address();
          logger.warn("Worker registration failed. Attempted to re-register worker at same address: " + workerAddress);
          context.reply(new RegisterWorkerFailed("Attempted to re-register worker at same address: " + workerAddress));
        }
      }
    }

    else if (content instanceof RegisterClient) {
      RegisterClient msg = (RegisterClient)content;
      if (state == RecoveryState.STANDBY) {
        context.reply(new MasterInStandby());
      } else if (idToClient.containsKey(msg.clientId)) {
        context.reply(new RegisterClientFailed("Duplicate client ID"));
      } else {
        ClientInfo clientInfo = new ClientInfo(msg.clientId, msg.host, msg.port, msg.client);
        if (registerClient(clientInfo)) {
          persistenceEngine.addClient(clientInfo);
          context.reply(new RegisteredClient(msg.clientId, self()));
        } else {
          RpcAddress clientAddress = clientInfo.endpoint.address();
          logger.warn("Client registration failed. Attempted to re-register client at same address: " + clientAddress);
          context.reply(new RegisterClientFailed("Attempted to re-register client at same address: " + clientAddress));
        }
      }
    }

    else if (content instanceof RegisterTask) {
      RegisterTask msg = (RegisterTask) content;
      if (addOrUpdateTask(msg.taskConf, false)) {
        context.reply(new RegisteredTask(msg.taskConf.getTaskId()));
      } else {
        logger.warn("Attempted to register task failed: " + msg.taskConf.getTaskId());
        context.reply(new RegisterTaskFailed("Attempted to register task failed: " + msg.taskConf.getTaskId()));
      }
    }

    else if (content instanceof RegisterTaskGroup) {
      RegisterTaskGroup msg = (RegisterTaskGroup)content;
      if (addOrUpdateTaskGroup(msg.taskGroup, false)) {
        context.reply(new RegisteredTaskGroup(msg.taskGroup.getTaskGroupId()));
      } else {
        logger.warn("Attempted to register task group failed: " + msg.taskGroup.getTaskGroupId());
        context.reply(new RegisterTaskGroupFailed("Attempted to register task group failed: " + msg.taskGroup.getTaskGroupId()));
      }
    }

    else if (content instanceof UnregisterTask) {
      UnregisterTask msg = (UnregisterTask)content;
      removeTask(msg.taskId);
      context.reply(new UnregisteredTask(msg.taskId));
    }

    else if (content instanceof UnregisterTaskGroup) {
      UnregisterTaskGroup msg = (UnregisterTaskGroup)content;
      removeTaskGroup(msg.taskGroupId);
      context.reply(new UnregisteredTaskGroup(msg.taskGroupId));
    }

    else if (content instanceof UpdateTask) {
      UpdateTask msg = (UpdateTask)content;
      addOrUpdateTask(msg.taskConf, true);
      context.reply(new UpdatedTask(msg.taskConf.getTaskId()));
    }

    else if (content instanceof UpdateTaskGroup) {
      UpdateTaskGroup msg = (UpdateTaskGroup)content;
      addOrUpdateTaskGroup(msg.taskGroup, true);
      context.reply(new UpdatedTaskGroup(msg.taskGroup.getTaskGroupId()));
    }

    else if (content instanceof KillTask) {
      KillTask msg = (KillTask)content;
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(msg.task.taskConf.getWorkerGroup());
      Future future = worker.endpoint.ask(new KillTask(msg.task));
      try {
        Object result = future.get(WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        context.reply(new KilledTask(msg.task, (String)result));
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void onDisconnected(RpcAddress address) {
    addressToWorker.get(address);
  }

  private boolean canCompleteRecovery() {
    boolean complete = true;
    for (WorkerInfo worker : workers) {
      if (worker.getState() == WorkerState.UNKNOWN) {
        complete = false;
        break;
      }
    }
    return complete;
  }

  private void beginRecovery(WorkerInfo[] storedWorkers, ClientInfo[] storedClients) {
    for (ClientInfo client : storedClients) {
      logger.info("Trying to recover client: " + client);
      try {
        registerClient(client);
        client.setState(ClientState.UNKNOWN);
        client.endpoint.send(new MasterChanged(self()));
      } catch (Exception e) {
        logger.info("Client {} had exception on reconnect", client.id);
      }
    }

    for (WorkerInfo worker : storedWorkers) {
      logger.info("Trying to recover worker: " + worker.id);
      try {
        registerWorker(worker);
        worker.setState(WorkerState.UNKNOWN);
        worker.endpoint.send(new MasterChanged(self()));
      } catch (Exception e) {
        logger.info("Worker {} had exception on reconnect", worker.id);
      }
    }
  }

  private void completeRecovery() {
    if (state != RecoveryState.RECOVERING) {
      return;
    }
    for (WorkerInfo worker : workers) {
      if (worker.getState() == WorkerState.UNKNOWN) {
        workers.remove(worker);
      }
    }
    for (ClientInfo client : clients) {
      if (client.getState() == ClientState.UNKNOWN) {
        clients.remove(client);
      }
    }

    state = RecoveryState.ALIVE;
    for (String workerGroup : workerGroups) {
      schedule(workerGroup);
    }
    logger.info("Recovery complete - resuming operations!");
  }

  private void schedule(String workerGroup) {
    if (state != RecoveryState.ALIVE) {
      return;
    }
    TaskInfo task = taskQueueContext.get2LaunchingTask(workerGroup);
    if (task != null) {
      WorkerInfo worker = workerScheduler.getLaunchTaskWorker(workerGroup);
      // TODO send task to worker
      worker.endpoint.send(new LaunchTask(task));
    }
  }

  private boolean addOrUpdateTask(TaskConf taskConf, boolean update) {
    boolean success;
    if (update) {
      success = taskQueueContext.updateCronTask(taskConf);
    } else {
      success = taskQueueContext.addCronTask(taskConf);
    }
    return success;
  }

  private boolean addOrUpdateTaskGroup(TaskGroup taskGroup, boolean update) {
    boolean success;
    if (update) {
      success = taskQueueContext.updateTaskGroup(taskGroup);
    } else {
      success = taskQueueContext.addTaskGroup(taskGroup);
    }
    return success;
  }

  private void removeTask(String taskId) {
    taskQueueContext.removeCronTask(taskId);
  }

  private void removeTaskGroup(String taskGroupId) {
    taskQueueContext.removeTaskGroup(taskGroupId);
  }

  private boolean registerWorker(WorkerInfo worker) {
    Set<WorkerInfo> toRemoveWorkers = Sets.newHashSet();
    for (WorkerInfo w : workers) {
      if (w.host.equals(worker.host) && w.port == worker.port && w.getState() == WorkerState.DEAD) {
        toRemoveWorkers.add(w);
      }
    }
    for (WorkerInfo w : toRemoveWorkers) {
      workers.remove(w);
    }
    RpcAddress workerAddress = worker.endpoint.address();
    if (addressToWorker.containsKey(workerAddress)) {
      WorkerInfo oldWorker = addressToWorker.get(workerAddress);
      if (oldWorker.getState() == WorkerState.UNKNOWN) {
        removeWorker(oldWorker);
      } else {
        logger.info("Attempted to re-register worker at same address: " + workerAddress);
        return false;
      }
    }
    workers.add(worker);
    idToWorker.put(worker.id, worker);
    addressToWorker.put(workerAddress, worker);
    return true;
  }

  private void removeWorker(WorkerInfo worker) {
    logger.info("Removing worker {} on {}:{}", worker.id, worker.host, worker.port);
    worker.setState(WorkerState.DEAD);
    idToWorker.remove(worker.id);
    addressToWorker.remove(worker.endpoint.address());
    persistenceEngine.removeWorker(worker);
  }

  private boolean registerClient(ClientInfo client) {
    Set<ClientInfo> toRemoveClients = Sets.newHashSet();
    for (ClientInfo c : clients) {
      if (c.host.equals(client.host) && c.port == client.port && c.getState() == ClientState.DEAD) {
        toRemoveClients.add(c);
      }
    }
    for (ClientInfo c : toRemoveClients) {
      clients.remove(c);
    }
    RpcAddress clientAddress = client.endpoint.address();
    if (addressToClient.containsKey(clientAddress)) {
      ClientInfo oldClient = addressToClient.get(clientAddress);
      if (oldClient.getState() == ClientState.UNKNOWN) {
        removeClient(oldClient);
      } else {
        logger.info("Attempted to re-register client at same address: " + clientAddress);
        return false;
      }
    }
    clients.add(client);
    idToClient.put(client.id, client);
    addressToClient.put(clientAddress, client);
    return true;
  }

  private void removeClient(ClientInfo client) {
    logger.info("Removing client {} on {}:{}", client.id, client.host, client.port);
    client.setState(ClientState.DEAD);
    idToClient.remove(client.getId());
    persistenceEngine.removeClient(client);
  }

  private void timeOutDeadWorkers() {
    long currentTime = System.currentTimeMillis();
    for (WorkerInfo w : workers) {
      if (w.getLastHeartbeat() < currentTime - WORKER_TIMEOUT_MS) {
        if (w.getState() != WorkerState.DEAD) {
          logger.warn("Removing {} because we got no heartbeat in {} seconds", w.id, WORKER_TIMEOUT_MS / 1000);
          removeWorker(w);
        } else {
          if (w.getLastHeartbeat() < (currentTime - (REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
            workers.remove(w);
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    Preconditions.checkArgument(args == null || args.length != 1,
      "Master must start with property file path param.");
    String propertyFilePath = args[0];
    DTSConf conf = DTSConfUtil.readFile(propertyFilePath);
    String host = conf.get("dts.master.host");
    int port = conf.getInt("dts.master.port", 8088);
    NettyRpcEnv rpcEnv = startRpcEnvAndEndpoint(host, port, conf);
    rpcEnv.awaitTermination();
  }

  private static NettyRpcEnv startRpcEnvAndEndpoint(String host, int port, DTSConf conf) {
    NettyRpcEnv rpcEnv = NettyRpcEnv.create(conf, host, port, false);
    TaskQueueContext context = new TaskQueueContext(conf);
    rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address(), conf, context));
    return rpcEnv;
  }
}
