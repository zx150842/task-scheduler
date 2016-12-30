package com.dts.core.worker;

import com.dts.core.TaskInfo;
import com.dts.core.master.Master;
import com.dts.core.util.ThreadUtils;
import com.dts.rpc.*;
import com.dts.rpc.netty.NettyRpcEnv;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class Worker extends RpcEndpoint {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final NettyRpcEnv rpcEnv;
  private final DTSConf conf;
  private RpcAddress[] masterRpcAddresses;

  private final String host;
  private final int port;
  private final int cores;
  private final long memory;
  private final ScheduledExecutorService forwordMessageScheduler;

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
  private final String workerId;
  private final String workerGroupId;

  private Future[] registerMasterFutrues = null;
  private ScheduledFuture registrationRetryTimer = null;
  private final ThreadPoolExecutor registerMasterThreadPool;
  private int connectionAttemptCount = 0;

  private int coresUsed = 0;
  private int memoryUsed = 0;

  public Worker(NettyRpcEnv rpcEnv, DTSConf conf) {
    super(rpcEnv);
    this.rpcEnv = rpcEnv;
    this.conf = conf;

    this.host = rpcEnv.address().getHost();
    this.port = rpcEnv.address().getPort();
    this.HEARTBEAT_MILLIS = conf.getLong("dts.worker.timeout", 60) * 1000 / 4;

    this.workerId = conf.get("dts.worker.id");
    this.workerGroupId = conf.get("dts.worker.groupId");

    OperatingSystemMXBean system = ManagementFactory.getOperatingSystemMXBean();
    MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapMemory = memory.getHeapMemoryUsage();

    this.cores = system.getAvailableProcessors();
    this.memory = heapMemory.getInit();

    this.forwordMessageScheduler =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler");
    this.registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
        "worker-register-master-threadpool", masterRpcAddresses.length, 60);

  }

  @Override
  public void onStart() {
    assert !registered;
    logger.info("Starting worker {}:{} with {} cores, {} RAM", host, port, cores, memory);
    // TODO add metrics
  }

  private void changeMaster(RpcEndpointRef master) {
    this.master = master;
    connected = true;
  }

  private void cancelLastRegistrationRetry() {
    if (registerMasterFutrues != null) {
      for (Future future : registerMasterFutrues) {
        future.cancel(true);
      }
      registerMasterFutrues = null;
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
      context.reply(new MasterChangeAckFromWorker(workerId, coresUsed, memoryUsed));
    }

    else if (o instanceof WorkerLastestState) {
      context.reply(new WorkerLastestState(workerId, coresUsed, memoryUsed));
    }

    else if (o instanceof LaunchTask) {
      LaunchTask msg = (LaunchTask)o;
      TaskInfo task = msg.task;
      // TODO schedule task
      context.reply(new LaunchTaskAck(task, workerId));
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
    forwordMessageScheduler.shutdownNow();
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
      forwordMessageScheduler.scheduleAtFixedRate(new Runnable() {
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
        if (registerMasterFutrues != null) {
          for (Future future : registerMasterFutrues) {
            future.cancel(true);
          }
          RpcAddress masterAddress = master.address();
          registerMasterFutrues = new Future[] {registerMasterThreadPool.submit(new Runnable() {
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
        if (registerMasterFutrues != null) {
          for (Future future : registerMasterFutrues) {
            future.cancel(true);
          }
          registerMasterFutrues = tryRegisterAllMasters();
        }
      }

      if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
        registrationRetryTimer = forwordMessageScheduler.scheduleAtFixedRate(new Runnable() {
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
      registerMasterFutrues = tryRegisterAllMasters();
      registrationRetryTimer = forwordMessageScheduler.scheduleAtFixedRate(new Runnable() {
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
        .ask(new RegisterWorker(workerId, host, port, self(), cores, memory, workerGroupId));
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
}
