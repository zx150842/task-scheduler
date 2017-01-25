package com.dts.core.worker;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.master.Master;
import com.dts.core.util.DataTypeUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.rpc.*;
import com.dts.rpc.exception.DTSException;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class Worker extends RpcEndpoint {
  private static final Logger logger = LoggerFactory.getLogger(Worker.class);

  private final DTSConf conf;
  private RpcAddress[] masterRpcAddresses;

  public final String workerId;
  public final String workerGroupId;
  public final String host;
  public final int port;
  public final int cores;
  public final long memory;

  private final ScheduledExecutorService forwardMessageScheduler;

  private final long HEARTBEAT_MILLIS;

  private final int INITIAL_REGISTRATION_RETRIES = 6;
  private final int TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10;
  private final float FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.5f;
  private final double REGISTRATION_RETRY_FUZZ_MULTIPLIER = new Random(UUID.randomUUID().getMostSignificantBits()).nextDouble() + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND;
  private final long INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = Math.round(10 * REGISTRATION_RETRY_FUZZ_MULTIPLIER);
  private final long PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = Math.round(60 * REGISTRATION_RETRY_FUZZ_MULTIPLIER);

  private RpcEndpointRef master;
  private boolean registered = false;
  private boolean connected = false;

  private Future[] registerMasterFutures = null;
  private ScheduledFuture registrationRetryTimer = null;
  private final ThreadPoolExecutor registerMasterThreadPool;
  private int connectionAttemptCount = 0;

  private int coresUsed = 0;
  private int memoryUsed = 0;

  private final TaskDispatcher taskDispatcher;
  private static final Gson GSON = new Gson();

  public static final String SYSTEM_NAME = "dtsWorker";
  public static final String ENDPOINT_NAME = "Worker";

  public Worker(RpcEnv rpcEnv, int cores, long memory, RpcAddress[] masterAddresses, DTSConf conf) {
    super(rpcEnv);
    this.conf = conf;

    this.host = rpcEnv.address().host;
    this.port = rpcEnv.address().port;
    this.HEARTBEAT_MILLIS = conf.getLong("dts.worker.timeout", 60) * 1000 / 4;

    this.workerId = conf.get("dts.worker.id");
    this.workerGroupId = conf.get("dts.worker.groupId");

    this.cores = cores;
    this.memory = memory;
    this.masterRpcAddresses = masterAddresses;

    this.forwardMessageScheduler =
        ThreadUtil.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler");
    this.registerMasterThreadPool = ThreadUtil.newDaemonCachedThreadPool(
        "worker-register-master-threadpool", masterRpcAddresses.length, 60);

    this.taskDispatcher = new TaskDispatcher(conf, self());
  }

  @Override
  public void onStart() {
    assert !registered;
    logger.info("Starting worker {}:{} with {} cores, {} RAM", host, port, cores, memory);
    taskDispatcher.onStart();
    // TODO add metrics
  }

  private void changeMaster(RpcEndpointRef master) {
    this.master = master;
    connected = true;
  }

  private void cancelLastRegistrationRetry() {
    if (registerMasterFutures != null) {
      for (Future future : registerMasterFutures) {
        future.cancel(true);
      }
      registerMasterFutures = null;
      registrationRetryTimer.cancel(true);
      registrationRetryTimer = null;
    }
  }

  @Override
  public void receive(Object o) {
    if (o instanceof SendHeartbeat) {
      if (connected) {
        sendToMaster(new Heartbeat(workerId, self()));
      }
    }

    else if (o instanceof ReconnectWorker) {
      ReconnectWorker msg = (ReconnectWorker) o;
      logger.info("Master {} requested this worker to reconnect", msg.masterUrl);
      registerWithMaster();
    }

    else if (o instanceof ReregisterWithMaster) {
      reregisterWithMaster();
    }
  }

  @Override
  public void receiveAndReply(Object o, RpcCallContext context) {
    if (o instanceof MasterChanged) {
      MasterChanged masterEndpoint = (MasterChanged) o;
      logger.info("Master has changed, new master is at {}", masterEndpoint.master.address());
      changeMaster(masterEndpoint.master);
//      context.reply(new MasterChangeAckFromWorker(workerId, coresUsed, memoryUsed));
    }

    else if (o instanceof RequestWorkerState) {
      context.reply(new WorkerLastestState(workerId, coresUsed, memoryUsed));
    }

    else if (o instanceof LaunchTask) {
      LaunchTask msg = (LaunchTask)o;
      List<Object> params = deserializeTaskParams(msg.task.getParams());
      TaskWrapper tw = new TaskWrapper(msg.task, params);
      // TODO schedule task
      String message;
      try {
        if (taskDispatcher.addTask(tw)) {
          message = "success";
        } else {
          message = "Failed to add task to worker task queue";
        }
      } catch (Exception e) {
        message = Throwables.getStackTraceAsString(e);
      }
      context.reply(new LaunchedTask(msg.task, message));
    }

    else if (o instanceof KillRunningTask) {
      KillRunningTask msg = (KillRunningTask)o;
      // TODO kill task
      String message;
      if (taskDispatcher.stopTask(msg.task.getSysId())) {
        message = "success";
      } else {
        message = "Failed to stop task: " + msg.task;
      }
      context.reply(new KilledTask(msg.task, message));
    }

    else {
      context.sendFailure(new DTSException("Worker not support receive msg type: " + o.getClass().getCanonicalName()));
    }
  }

  @Override
  public void onDisconnected(RpcAddress remoteAddress) {
    if (master.address() == remoteAddress) {
      logger.info("{} Disassociated", remoteAddress);
      logger.error("Connection to master failed! Waiting for master to reconnect");
      connected = false;
      registerWithMaster();
    }
  }

  @Override
  public void onStop() {
    forwardMessageScheduler.shutdownNow();
    registerMasterThreadPool.shutdownNow();
  }

  private void sendToMaster(Object msg) {
    if (msg != null) {
      master.send(msg);
    } else {
      logger.warn("Dropping {} because the connection to master has not yet been established");
    }
  }

  private synchronized void handleRegisterResponse(RegisterResponse rsp) {
    if (rsp instanceof RegisteredWorker) {
      RegisteredWorker msg = (RegisteredWorker) rsp;
      RpcEndpointRef masterEndpoint = msg.master;
      logger.info("Successfully registered with master {}", masterEndpoint.address());
      registered = true;
      changeMaster(masterEndpoint);
      forwardMessageScheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          self().send(new SendHeartbeat());
        }
      }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS);
      masterEndpoint.send(new WorkerLastestState(workerId, coresUsed, memoryUsed));
    }

    else if (rsp instanceof RegisterWorkerFailed) {
      RegisterWorkerFailed msg = (RegisterWorkerFailed) rsp;
      logger.error("Worker registration failed: " + msg.message);
      System.exit(1);
    }

    else if (rsp instanceof MasterInStandby) {
      // Ignore
    }
  }

  private void reregisterWithMaster() {
    connectionAttemptCount += 1;
    if (registered) {
      cancelLastRegistrationRetry();
    } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
      logger.info("Retrying connection to master (attempt # {})", connectionAttemptCount);
      if (master != null) {
        if (registerMasterFutures != null) {
          for (Future future : registerMasterFutures) {
            future.cancel(true);
          }
          RpcAddress masterAddress = master.address();
          registerMasterFutures = new Future[] {registerMasterThreadPool.submit(new Runnable() {
            @Override
            public void run() {
              try {
                logger.info("Connecting to master {}", masterAddress);
                RpcEndpointRef masterEndpoint =
                    rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME);
                registerWithMaster(masterEndpoint);
              } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                  // Cancelled
                } else {
                  logger.warn("Failed to connect to master {}", masterAddress);
                }
              }
            }
          })};
        }
      } else {
        if (registerMasterFutures != null) {
          for (Future future : registerMasterFutures) {
            future.cancel(true);
          }
          registerMasterFutures = tryRegisterAllMasters();
        }
      }

      if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
        registrationRetryTimer = forwardMessageScheduler.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            self().send(new ReregisterWithMaster());
          }
        }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
            PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS, TimeUnit.SECONDS);
      }
    } else {
      logger.error("All masters are unresponsive! Giving up.");
      System.exit(1);
    }
  }

  private void registerWithMaster() {
    if (registrationRetryTimer == null) {
      registered = false;
      registerMasterFutures = tryRegisterAllMasters();
      registrationRetryTimer = forwardMessageScheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          self().send(new ReregisterWithMaster());
        }
      }, INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS, INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS);
    } else {
      logger.info("Not spawning another attempt to register with the master, since there is an"
          + " attempt scheduled already");
    }
  }

  private void registerWithMaster(RpcEndpointRef masterEndpoint) {
    Future future = masterEndpoint
        .ask(new RegisterWorker(workerId, self(), cores, memory, workerGroupId));
    try {
      RegisterResponse msg = (RegisterResponse) future.get();
      handleRegisterResponse(msg);
    } catch (Throwable e) {
      logger.error("Cannot register with master: {}", masterEndpoint.address());
      System.exit(1);
    }
  }

  private Future[] tryRegisterAllMasters() {
    List<Future> futures = Lists.newArrayList();
    for (RpcAddress masterAddress : masterRpcAddresses) {
      Future future = registerMasterThreadPool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            logger.info("Connecting to master {}", masterAddress);
            RpcEndpointRef masterEndpoint =
                rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME);
            registerWithMaster(masterEndpoint);
          } catch (Throwable e) {
            logger.warn("Failed to connect to master {}", masterAddress);
          }
        }
      });
      futures.add(future);
    }
    return futures.toArray(new Future[futures.size()]);
  }

  public static Worker launchWorker(
    String host, int port, String[] masterUrls, DTSConf conf) {
    RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, false);
    List<RpcAddress> masterAddresses = Lists.newArrayList();
    for (String masterUrl : masterUrls) {
      RpcAddress masterAddress = RpcAddress.fromURL(masterUrl);
      if (masterAddress == null) {
        logger.warn("Ignore masterUrl: {}, masterUrl format should be host:port", masterUrl);
        continue;
      }
      masterAddresses.add(masterAddress);
    }

    OperatingSystemMXBean system = ManagementFactory.getOperatingSystemMXBean();
    MemoryUsage heapMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();

    int cores = system.getAvailableProcessors();
    long memory = heapMemory.getInit();

    Worker worker = new Worker(rpcEnv, cores, memory, masterAddresses.toArray(new RpcAddress[0]), conf);
    rpcEnv.setupEndpoint(ENDPOINT_NAME, worker);
    return worker;
  }

  private List<Object> deserializeTaskParams(String params) {
    List<Object> paramValues = Lists.newArrayList();
    if (StringUtils.isBlank(params)) {
      return paramValues;
    }
    LinkedHashMap<String, String> typeValues = GSON.fromJson(params, new TypeToken<LinkedHashMap<String, String>>(){}.getType());
    for (String type : typeValues.keySet()) {
      paramValues.add(DataTypeUtil.convertToPrimitiveType(type, typeValues.get(type)));
    }
    return paramValues;
  }
}
