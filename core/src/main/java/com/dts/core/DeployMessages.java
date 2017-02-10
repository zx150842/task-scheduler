package com.dts.core;

import com.dts.core.rpc.RpcEndpointRef;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class DeployMessages implements Serializable {
  private static final long serialVersionUID = 7950453821489411446L;

  // Worker to Master

  public static class RegisterWorker implements Serializable {
    private static final long serialVersionUID = 5328508385791023709L;
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
    private static final long serialVersionUID = 7089091427436269671L;
    public final TriggeredTaskInfo task;
    public final String message;

    public LaunchedTask(TriggeredTaskInfo task, String message) {
      this.task = task;
      this.message = message;
    }
  }

  public static class ExecutingTask implements Serializable {
    private static final long serialVersionUID = -1327369516594181690L;
    public final TriggeredTaskInfo task;
    public final String executingThread;

    public ExecutingTask(TriggeredTaskInfo task, String executingThread) {
      this.task = task;
      this.executingThread = executingThread;
    }
  }

  public static class FinishTask implements Serializable {
    private static final long serialVersionUID = -50490690996843513L;
    public final TriggeredTaskInfo task;
    public final String message;

    public FinishTask(TriggeredTaskInfo task, String message) {
      this.task = task;
      this.message = message;
    }
  }

  public static class KilledTask implements Serializable {
    private static final long serialVersionUID = 4829266027002849504L;
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
    private static final long serialVersionUID = -2465127500865016137L;
    public final TriggeredTaskInfo task;

    public LaunchTask(TriggeredTaskInfo task) {
      this.task = task;
    }
  }

  public static class KillRunningTask implements Serializable {
    private static final long serialVersionUID = 69888105641180689L;
    public final TriggeredTaskInfo task;

    public KillRunningTask(TriggeredTaskInfo task) {
      this.task = task;
    }
  }

  public interface RegisterResponse extends Serializable {}

  public static class MasterInStandby implements RegisterResponse {}

  // Worker internal
  public static class ReregisterWithMaster implements Serializable {}

  // Client to Master

  public static class AskMaster implements Serializable {
    private static final long serialVersionUID = 1459971664502308297L;
  }

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
    private static final long serialVersionUID = 5274690913642148850L;
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
