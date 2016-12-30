package com.dts.core;

import com.dts.rpc.RpcEndpointRef;

/**
 * @author zhangxin
 */
public class DeployMessages {

  // Worker to Master

  public static class RegisterWorker {
    public final String workerId;
    public final String host;
    public final int port;
    public final RpcEndpointRef worker;
    public final int cores;
    public final long memory;
    public final String groupId;

    public RegisterWorker(
      String workerId,
      String host,
      int port,
      RpcEndpointRef worker,
      int cores,
      long memory,
      String groupId) {
      this.workerId = workerId;
      this.host = host;
      this.port = port;
      this.worker = worker;
      this.cores = cores;
      this.memory = memory;
      this.groupId = groupId;
    }
  }

  public static class Heartbeat {
    public final String workerId;
    public final RpcEndpointRef worker;

    public Heartbeat(String workerId, RpcEndpointRef worker) {
      this.workerId = workerId;
      this.worker = worker;
    }
  }

  public static class WorkerLastestState {
    public final String workerId;
    public final int coreUsed;
    public final int memoryUsed;

    public WorkerLastestState(String workerId, int coreUsed, int memoryUsed) {
      this.workerId = workerId;
      this.coreUsed = coreUsed;
      this.memoryUsed = memoryUsed;
    }
  }

  public static class MasterChangeAckFromWorker {
    public final String workerId;
    public final int coreUsed;
    public final int memoryUsed;

    public MasterChangeAckFromWorker(String workerId, int coreUsed, int memoryUsed) {
      this.workerId = workerId;
      this.coreUsed = coreUsed;
      this.memoryUsed = memoryUsed;
    }
  }

  public static class TaskResult {
    public final String taskId;
    public final TaskStatus status;

    public TaskResult(String taskId, TaskStatus status) {
      this.taskId = taskId;
      this.status = status;
    }
  }

  public static class SendHeartbeat {}

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

  public static class ReconnectWorker {
    public final String masterUrl;

    public ReconnectWorker(String masterUrl) {
      this.masterUrl = masterUrl;
    }
  }

  public static class RequestWorkerState {}

  public static class LaunchTask {
    public final TaskInfo task;

    public LaunchTask(TaskInfo task) {
      this.task = task;
    }
  }

  public static class LaunchTaskAck {
    public final TaskInfo task;
    public final String workerId;

    public LaunchTaskAck(TaskInfo task, String workerId) {
      this.task = task;
      this.workerId = workerId;
    }
  }

  public interface RegisterResponse {}

  // Master to Worker & Client

  public static class MasterChanged {
    public final RpcEndpointRef master;

    public MasterChanged(RpcEndpointRef master) {
      this.master = master;
    }
  }

  public static class MasterInStandby implements RegisterResponse {}

  // Worker internal
  public static class ReregisterWithMaster {}

  // Client to Master

  public static class RegisterClient {
    public final String clientId;
    public final String host;
    public final int port;
    public final RpcEndpointRef client;

    public RegisterClient(String clientId, String host, int port, RpcEndpointRef client) {
      this.clientId = clientId;
      this.host = host;
      this.port = port;
      this.client = client;
    }
  }

  public static class RegisterTask {
    public final TaskInfo task;

    public RegisterTask(TaskInfo task) {
      this.task = task;
    }
  }

  public static class RegisterTaskGroup {
    public final TaskGroup taskGroup;

    public RegisterTaskGroup(TaskGroup taskGroup) {
      this.taskGroup = taskGroup;
    }
  }

  public static class UpdateTask {
    public final TaskInfo task;

    public UpdateTask(TaskInfo task) { this.task = task; }
  }

  public static class UpdateTaskGroup {
    public final TaskGroup taskGroup;

    public UpdateTaskGroup(TaskGroup taskGroup) { this.taskGroup = taskGroup; }
  }

  public static class UnregisterTask {
    public final String taskId;

    public UnregisterTask(String taskId) {
      this.taskId = taskId;
    }
  }

  public static class UnregisterTaskGroup {
    public final String taskGroupId;

    public UnregisterTaskGroup(String taskGroupId) {
      this.taskGroupId = taskGroupId;
    }
  }

  public static class ManualTriggerTask {
    public final String taskId;

    public ManualTriggerTask(String taskId) {
      this.taskId = taskId;
    }
  }

  public static class ManualTriggerTaskGroup {
    public final String taskGroupId;

    public ManualTriggerTaskGroup(String taskGroupId) {
      this.taskGroupId = taskGroupId;
    }
  }

  public static class MasterChangeAckFromClient {
    public final String clientId;

    public MasterChangeAckFromClient(String clientId) {
      this.clientId = clientId;
    }
  }

  // Master to Client

  public static class RegisteredClient {
    public final String clientId;
    public final RpcEndpointRef master;

    public RegisteredClient(String clientId, RpcEndpointRef master) {
      this.clientId = clientId;
      this.master = master;
    }
  }

  public static class RegisterClientFailed {
    public final String message;

    public RegisterClientFailed(String message) {
      this.message = message;
    }
  }

  public static class RegisteredTask {
    public final String taskId;

    public RegisteredTask(String taskId) {
      this.taskId = taskId;
    }
  }

  public static class RegisteredTaskGroup {
    public final String taskGroupId;

    public RegisteredTaskGroup(String taskGroupId) {
      this.taskGroupId = taskGroupId;
    }
  }

  public static class UnregisteredTask {
    public final String taskId;

    public UnregisteredTask(String taskId) {
      this.taskId = taskId;
    }
  }

  public static class UnregisteredTaskGroup {
    public final String taskGroupId;

    public UnregisteredTaskGroup(String taskGroupId) {
      this.taskGroupId = taskGroupId;
    }
  }

  public static class UpdatedTask {
    public final String taskId;

    public UpdatedTask(String taskId) { this.taskId = taskId; }
  }

  public static class UpdatedTaskGroup {
    public final String taskGroupId;

    public UpdatedTaskGroup(String taskGroupId) { this.taskGroupId = taskGroupId; }
  }
}
