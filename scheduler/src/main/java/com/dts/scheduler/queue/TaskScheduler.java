package com.dts.scheduler.queue;

import com.dts.core.DTSConf;
import com.dts.core.TriggeredTaskInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 任务调度器，用来选择一个任务以下发到worker上执行
 *
 * @author zhangxin
 */
public class TaskScheduler {
  private final DTSConf conf;

  private final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);
  private final ExecutableTaskQueue executableTaskQueue;
  private final ExecutingTaskQueue executingTaskQueue;

  private final int PARALLEL_RUNNING_TASK;

  public TaskScheduler(DTSConf conf,
    ExecutableTaskQueue executableTaskQueue,
    ExecutingTaskQueue executingTaskQueue) {
    this.conf = conf;
    this.executableTaskQueue = executableTaskQueue;
    this.executingTaskQueue = executingTaskQueue;
    this.PARALLEL_RUNNING_TASK = conf.getInt("dts.task.parallel.run.num", 1);
  }

  /**
   * 从可执行任务队列中选择一个任务下发到worker执行
   * <p>手动触发的任务队列优先级高于自动执行的任务队列</p>
   *
   * @param workerGroup
   * @return
   */
  public TriggeredTaskInfo schedule(String workerGroup) {
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
      if (runningTasks != null && PARALLEL_RUNNING_TASK > 0 && runningTasks.size() > PARALLEL_RUNNING_TASK) {
        logger.warn("Task {} current running task is {} over 'dts.task.parallel.run.num' {}",
          task.getTaskId() + "-" + task.getTaskName(), runningTasks.size(), PARALLEL_RUNNING_TASK);
        continue;
      }
      return task;
    }
    return null;
  }
}
