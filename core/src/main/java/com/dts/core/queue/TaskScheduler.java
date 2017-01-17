package com.dts.core.queue;

import com.dts.core.TaskInfo;
import com.dts.rpc.DTSConf;

import java.util.List;

/**
 * @author zhangxin
 */
public class TaskScheduler {

  private final DTSConf conf;
  private final ExecutableTaskQueue executableTaskQueue;
  private final LaunchingTaskQueue launchingTaskQueue;

  public TaskScheduler(DTSConf conf,
    ExecutableTaskQueue executableTaskQueue,
    LaunchingTaskQueue launchingTaskQueue) {
    this.conf = conf;
    this.executableTaskQueue = executableTaskQueue;
    this.launchingTaskQueue = launchingTaskQueue;
  }

  public TaskInfo schedule(String workerGroup) {
    int taskParallelRunNum = conf.getInt("dts.task.parallel.run.num", 1);
    List<TaskInfo> tasks = executableTaskQueue.getManualTriggerTasks(workerGroup);
    if (tasks != null) {
      return tasks.get(0);
    }
    tasks = executableTaskQueue.getAutoTriggerTasks(workerGroup);
    for (TaskInfo task : tasks) {
      List<TaskInfo> runningTasks = launchingTaskQueue.getByTaskId(task.taskConf.getTaskId());
      if (runningTasks != null && runningTasks.size() > taskParallelRunNum) {
        continue;
      }
      return task;
    }
    return null;
  }

}
