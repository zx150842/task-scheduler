package com.dts.core;

import com.dts.core.rpc.RpcEndpointRef;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class DeployMessages implements Serializable {

  // Worker to Master

  public static class RegisterWorker implements Serializable {
    public final String workerId;
    public final int cores;
    public final long memory;
    public final String groupId;
    public final RpcEndpointRef worker;

    public RegisterWorker(
      String workerId,
      RpcEndpointRef worker,
      int cores,
      long memory,
      String groupId) {
      this.workerId = workerId;
      this.worker = worker;
      this.cores = cores;
      this.memory = memory;
      this.groupId = groupId;
    }
  }

  public static class Heartbeat implements Serializable {
    public final String workerId;
    public final RpcEndpointRef worker;

    public Heartbeat(String workerId, RpcEndpointRef worker) {
      this.workerId = workerId;
      this.worker = worker;
    }
  }

  public static class WorkerLastestState implements Serializable {
    public final String workerId;
    public final int coreUsed;
    public final int memoryUsed;

    public WorkerLastestState(String workerId, int coreUsed, int memoryUsed) {
      this.workerId = workerId;
      this.coreUsed = coreUsed;
      this.memoryUsed = memoryUsed;
    }
  }

  public static class SendHeartbeat implements Serializable {}

  public static class LaunchedTask implements Serializable {
    public final TriggeredTaskInfo task;
    public final String message;

    public LaunchedTask(TriggeredTaskInfo task, String message) {
      this.task = task;
      this.message = message;
    }
  }

  public static class ExecutingTask implements Serializable {
    public final TriggeredTaskInfo task;
    public final String executingThread;

    public ExecutingTask(TriggeredTaskInfo task, String executingThread) {
      this.task = task;
      this.executingThread = executingThread;
    }
  }

  public static class FinishTask implements Serializable {
    public final TriggeredTaskInfo task;
    public final String message;

    public FinishTask(TriggeredTaskInfo task, String message) {
      this.task = task;
      this.message = message;
    }
  }

  public static class KilledTask implements Serializable {
    public final TriggeredTaskInfo task;
    public final String message;

    public KilledTask(TriggeredTaskInfo task, String message) {
      this.task = task;
      this.message = message;
    }
  }

  // Master to Worker

  public static class RegisteredWorker implements RegisterResponse {
    public final RpcEndpointRef master;

    public RegisteredWorker(RpcEndpointRef master) {
      this.master = master;
    }
  }

  public static class RegisterWorkerFailed implements RegisterResponse {
    public final String message;

    public RegisterWorkerFailed(String message) {
      this.message = message;
    }
  }

  public static class ReconnectWorker implements Serializable {
    public final String masterUrl;

    public ReconnectWorker(String masterUrl) {
      this.masterUrl = masterUrl;
    }
  }

  public static class RequestWorkerState implements Serializable {}

  public static class LaunchTask implements Serializable {
    public final TriggeredTaskInfo task;

    public LaunchTask(TriggeredTaskInfo task) {
      this.task = task;
    }
  }

  public static class KillRunningTask implements Serializable {
    public final TriggeredTaskInfo task;

    public KillRunningTask(TriggeredTaskInfo task) {
      this.task = task;
    }
  }

  public interface RegisterResponse extends Serializable {}

  // Master to Worker & Client

  public static class MasterChanged implements Serializable {
    public final RpcEndpointRef master;

    public MasterChanged(RpcEndpointRef master) {
      this.master = master;
    }
  }

  public static class MasterInStandby implements RegisterResponse {}

  // Worker internal
  public static class ReregisterWithMaster implements Serializable {}

  // Client to Master

  public static class RegisterJob implements Serializable {
    public final JobConf jobConf;

    public RegisterJob(JobConf jobConf) {
      this.jobConf = jobConf;
    }
  }

  public static class UpdateJob implements Serializable {
    public final JobConf jobConf;

    public UpdateJob(JobConf jobConf) { this.jobConf = jobConf; }
  }

  public static class UnregisterJob implements Serializable {
    public final String jobId;

    public UnregisterJob(String jobId) {
      this.jobId = jobId;
    }
  }

  public static class ManualTriggerJob implements Serializable {
    public final JobConf jobConf;

    public ManualTriggerJob(JobConf jobConf) {
      this.jobConf = jobConf;
    }
  }

  // Master to Client

  public static class RegisteredClient implements Serializable {
    public final String clientId;
    public final RpcEndpointRef master;

    public RegisteredClient(String clientId, RpcEndpointRef master) {
      this.clientId = clientId;
      this.master = master;
    }
  }

  public static class RegisterClientFailed implements Serializable {
    public final String message;

    public RegisterClientFailed(String message) {
      this.message = message;
    }
  }

  public static class RegisteredJob implements Serializable {
    public final String jobId;

    public RegisteredJob(String jobId) {
      this.jobId = jobId;
    }
  }

  public static class RegisterJobFailed implements Serializable {
    public final String message;

    public RegisterJobFailed(String message) {
      this.message = message;
    }
  }

  public static class UnregisteredJob implements Serializable {
    public final String jobId;

    public UnregisteredJob(String jobId) {
      this.jobId = jobId;
    }
  }

  public static class UpdatedJob implements Serializable {
    public final String jobId;

    public UpdatedJob(String jobId) { this.jobId = jobId; }
  }
}
