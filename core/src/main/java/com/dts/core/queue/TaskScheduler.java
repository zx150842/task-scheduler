package com.dts.core.queue;

import com.dts.core.TaskInfo;
import com.dts.rpc.DTSConf;

import java.util.List;
import java.util.Set;

/**
 * @author zhangxin
 */
public class TaskScheduler {

  private final DTSConf conf;
  private final ExecutableTaskQueue executableTaskQueue;
  private final ExecutingTaskQueue executingTaskQueue;

  public TaskScheduler(DTSConf conf,
    ExecutableTaskQueue executableTaskQueue,
    ExecutingTaskQueue executingTaskQueue) {
    this.conf = conf;
    this.executableTaskQueue = executableTaskQueue;
    this.executingTaskQueue = executingTaskQueue;
  }

  public TaskInfo schedule(String workerGroup) {
    int taskParallelRunNum = conf.getInt("dts.task.parallel.run.num", 1);
    List<TaskInfo> tasks = executableTaskQueue.getManualTriggerTasks(workerGroup);
    if (tasks != null) {
      return tasks.get(0);
    }
    tasks = executableTaskQueue.getAutoTriggerTasks(workerGroup);
    for (TaskInfo task : tasks) {
      List<TaskInfo> runningTasks = executingTaskQueue.getByTaskId(task.getTaskId());
      if (runningTasks != null && runningTasks.size() > taskParallelRunNum) {
        continue;
      }
      return task;
    }
    return null;
  }

}
