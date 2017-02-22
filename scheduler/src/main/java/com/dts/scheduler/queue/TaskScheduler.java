package com.dts.scheduler.queue;

import com.dts.core.DTSConf;
import com.dts.core.TriggeredTaskInfo;

import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 任务调度器，用来选择一个任务以下发到worker上执行
 *
 * @author zhangxin
 */
public class TaskScheduler {

  private final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);
  private final LinkedBlockingDeque<TriggeredTaskInfo> executableTasks;
  private final Map<String, String> lastExecuteTaskSysIdMap;
  private final ListMultimap<String, TriggeredTaskInfo> executingTasks;
  private final TaskQueueContext context;

  private final int PARALLEL_RUNNING_TASK;

  public TaskScheduler(DTSConf conf, TaskQueueContext context) {
    this.PARALLEL_RUNNING_TASK = conf.getInt("dts.task.parallel.run.num", 1);
    this.context = context;
    this.executableTasks = context.executableTaskCache;
    this.lastExecuteTaskSysIdMap = context.lastExecuteTaskSysIdCache;
    this.executingTasks = context.executingTaskCache;
  }

  /**
   * 从可执行任务队列中选择一个任务下发到worker执行
   * <p>手动触发的任务队列优先级高于自动执行的任务队列</p>
   *
   * @return
   */
  public TriggeredTaskInfo schedule() throws InterruptedException {
    TriggeredTaskInfo task = executableTasks.take();
    if (task.isManualTrigger()) {
      return task;
    }
    String taskId = task.getTaskId();
    String lastExecuteTaskSysId = lastExecuteTaskSysIdMap.get(taskId);
    if (lastExecuteTaskSysId != null && task.getSysId().compareTo(lastExecuteTaskSysId) < 0) {
      context.skipTask(task);
      logger.warn("Current scheduled task sysId {} is before taskId {} last executing task sysId {}, "
          + "drop current task", task.getSysId(), taskId, lastExecuteTaskSysId);
      return null;
    }
    if (!executingTasks.containsKey(taskId)) {
      return task;
    }
    int executingTaskNum = executingTasks.get(taskId).size();
    if (PARALLEL_RUNNING_TASK > 0 && executingTaskNum > PARALLEL_RUNNING_TASK) {
      executableTasks.offer(task);
      logger.warn("Task {} current running task is {} over 'dts.task.parallel.run.num' {}",
        task.getTaskId() + "-" + task.getTaskName(), executingTaskNum, PARALLEL_RUNNING_TASK);
      return null;
    }
    return task;
  }
}
