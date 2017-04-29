package com.dts.core;

import com.dts.core.registration.NodeDetail;
import com.dts.core.registration.RpcRegisterMessage;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * @author zhangxin
 */
public class DeployMessages implements Serializable {
  private static final long serialVersionUID = 7950453821489411446L;
  public static final int SUCCESS = 1;
  public static final int FAIL = 0;

  // Worker to Master

  public static class FinishTask implements Serializable {
    private static final long serialVersionUID = -50490690996843513L;
    public final TriggeredTaskInfo task;
    public final String message;

    public FinishTask(TriggeredTaskInfo task, String message) {
      this.task = task;
      this.message = message;
    }
  }

  public static class AskMaster implements Serializable {
    private static final long serialVersionUID = 1459971664502308297L;
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

  // Client to Master

  public static class AskLeader implements Serializable {
    private static final long serialVersionUID = 1459971664502308297L;
  }

  public static class AskWorkers implements Serializable {
    private static final long serialVersionUID = -3717091659040505698L;
  }

  public static class ManualTriggerJob implements Serializable {
    private static final long serialVersionUID = 5274690913642148850L;
    public final boolean runOnSeed;
    public final JobConf jobConf;

    public ManualTriggerJob(JobConf jobConf, boolean runOnSeed) {
      this.runOnSeed = runOnSeed;
      this.jobConf = jobConf;
    }
  }

  public static class RefreshWorkers implements Serializable {
    private static final long serialVersionUID = -4607494485448365277L;
  }

  public static class RefreshJobs implements Serializable {
    private static final long serialVersionUID = 5746911972309297081L;
  }

  // Master to Client

  public static class ReplyWorkers implements Serializable {
    private static final long serialVersionUID = -7917674692550046906L;
    public final List<RpcRegisterMessage> workers;

    public ReplyWorkers(List<RpcRegisterMessage> workers) {
      this.workers = workers;
    }
  }

  public static class RefreshedJobs implements Serializable {
    private static final long serialVersionUID = -1177593892000985198L;
    public final int code;
    public final String message;

    public RefreshedJobs(int code, String message) {
      this.code = code;
      this.message = message;
    }
  }

  public static class ManualTriggeredJob implements Serializable {
    private static final long serialVersionUID = 8138460448142536202L;
    public final String id;

    public ManualTriggeredJob(String id) {
      this.id = id;
    }
  }
}
