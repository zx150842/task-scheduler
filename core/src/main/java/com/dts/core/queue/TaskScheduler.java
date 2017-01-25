package com.dts.core.queue;

import com.dts.core.DeployMessages;
import com.dts.core.TriggeredTaskInfo;
import com.dts.rpc.DTSConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhangxin
 */
public class TaskScheduler {
  private final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);

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

  public TriggeredTaskInfo schedule(String workerGroup) {
    int taskParallelRunNum = conf.getInt("dts.task.parallel.run.num", 1);
    List<TriggeredTaskInfo> tasks = executableTaskQueue.getManualTriggerTasks(workerGroup);
    if (tasks != null && tasks.size() > 0) {
      return tasks.get(0);
    }
    tasks = executableTaskQueue.getAutoTriggerTasks(workerGroup);
    if (tasks == null || tasks.isEmpty()) {
      return null;
    }
    for (TriggeredTaskInfo task : tasks) {
      List<TriggeredTaskInfo> runningTasks = executingTaskQueue.getByTaskId(task.getTaskId());
      if (runningTasks != null && runningTasks.size() > taskParallelRunNum) {
        logger.warn("Task {} current running task is {} over 'dts.task.parallel.run.num' {}",
          runningTasks.size(), taskParallelRunNum);
        continue;
      }
      return task;
    }
    return null;
  }

}
