package com.dts.scheduler.queue;

import com.dts.core.DTSConf;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.TaskStatus;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.util.IdUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.scheduler.queue.mysql.MysqlFinishedTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlLaunchedTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlLaunchingTaskQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.dts.scheduler.queue.mysql.MysqlCronTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlExecutableTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlExecutingTaskQueue;

/**
 * 任务队列上下文，master通过这个类与任务队列进行交互
 *
 * @author zhangxin
 */
public class TaskQueueContext {
  private final Logger logger = LoggerFactory.getLogger(TaskQueueContext.class);

  private final DTSConf conf;
  private final CronTaskQueue cronTaskQueue;
  private final ExecutableTaskQueue executableTaskQueue;
  private final LaunchingTaskQueue launchingTaskQueue;
  private final LaunchedTaskQueue launchedTaskQueue;
  private final ExecutingTaskQueue executingTaskQueue;
  private final FinishedTaskQueue finishedTaskQueue;
  private final TaskScheduler taskScheduler;

  private final ScheduledExecutorService backend;

  private final long TASK_TRIGGER_INTERVAL_MS;

  public TaskQueueContext(DTSConf conf) {
    this.conf = conf;
    TASK_TRIGGER_INTERVAL_MS = conf.getLong("dts.task.trigger.intervalMs", 1000);
    String queueType = conf.get("dts.task.queue.type", "mysql");
   if ("mysql".equals(queueType)) {
      cronTaskQueue = new MysqlCronTaskQueue(conf);
      executableTaskQueue = new MysqlExecutableTaskQueue();
      launchingTaskQueue = new MysqlLaunchingTaskQueue();
      launchedTaskQueue = new MysqlLaunchedTaskQueue();
      executingTaskQueue = new MysqlExecutingTaskQueue();
      finishedTaskQueue = new MysqlFinishedTaskQueue();
    } else {
      throw new IllegalArgumentException("Not Support 'dts.task.queue.type'= " + queueType);
    }
    this.taskScheduler = new TaskScheduler(conf, executableTaskQueue, executingTaskQueue);
    this.backend = ThreadUtil.newDaemonSingleThreadScheduledExecutor("task-generator");
  }

  public void start() {
    backend.scheduleAtFixedRate(new CronTaskGenerator(),0L, TASK_TRIGGER_INTERVAL_MS / 2, TimeUnit.MILLISECONDS);
  }

  public synchronized TriggeredTaskInfo get2LaunchingTask(String workerGroup) {
    TriggeredTaskInfo task = taskScheduler.schedule(workerGroup);
    if (task != null) {
      launchingTaskQueue.add(task);
      executableTaskQueue.remove(task.getSysId());
    }
    return task;
  }

  public synchronized boolean updateTaskWorkerId(String sysId, String workerId) {
    boolean success;
    if (sysId != null && workerId != null) {
      success = launchingTaskQueue.updateWorkerId(sysId, workerId);
    } else {
      success = false;
    }
    return success;
  }

  public synchronized boolean launchedTask(TriggeredTaskInfo task) {
    boolean success;
    if (task != null) {
      success = launchedTaskQueue.add(task);
      if (success) {
        success = launchingTaskQueue.remove(task.getSysId());
      }
    } else {
      success = false;
    }
    return success;
  }

  public synchronized boolean executingTask(TriggeredTaskInfo task) {
    boolean success;
    if (task != null) {
      success = executingTaskQueue().add(task);
      if (success) {
        launchedTaskQueue.remove(task.getSysId());
      }
    } else {
      success = false;
    }
    return success;
  }

  public synchronized void completeTask(String sysId) {
    TriggeredTaskInfo task = executingTaskQueue.getBySysId(sysId);
    if (task == null) {
      logger.error("executing queue has no task, sysId: {}", sysId);
      return;
    }
    finishedTaskQueue.add(task);
    executingTaskQueue.remove(task.getSysId());
    if (task.getStatus() != TaskStatus.SUCCESS) {
      executableTaskQueue.resume(task);
      return;
    }
    TaskConf nextTaskConf = cronTaskQueue.getNextToTriggerTask(task.getJobId(), task.getTaskName());
    if (nextTaskConf != null) {
      TriggeredTaskInfo nextTask = generateTriggerTask(nextTaskConf, task.getJobId(),
          task.getWorkerGroup(), new Timestamp(System.currentTimeMillis()), false);
      executableTaskQueue.add(nextTask);
    }
  }

  public synchronized boolean manualTriggerJob(JobConf manualTriggerJobConf) {
    String jobId = manualTriggerJobConf.getJobId();
    if (cronTaskQueue.containJob(jobId)) {
      JobConf jobConf = manualTriggerJobConf;
      if (jobConf == null || jobConf.getTasks() == null || jobConf.getTasks().isEmpty()) {
        jobConf = cronTaskQueue.getJob(jobId);
      }
      if (jobConf == null || jobConf.getTasks() == null || jobConf.getTasks().isEmpty()) {
        logger.error("Cannot trigger job {} with no task to execute", jobId);
        return false;
      }
      TaskConf taskConf = jobConf.getTasks().get(0);
      TriggeredTaskInfo task = generateTriggerTask(taskConf, jobId, jobConf.getWorkerGroup(),
          new Timestamp(System.currentTimeMillis()), true);
      executableTaskQueue.add(task);
      return true;
    }
    return false;
  }

  public ExecutingTaskQueue executingTaskQueue() {
    return executingTaskQueue;
  }

  public DTSConf getConf() {
    return conf;
  }

  private TriggeredTaskInfo generateTriggerTask(
      TaskConf taskConf,
      String jobId,
      String workerGroup,
      Date triggerTime,
      boolean manualTrigger) {
    TriggeredTaskInfo task = new TriggeredTaskInfo(jobId, workerGroup, taskConf.getTaskId(),
      taskConf.getTaskName(), taskConf.getParams(), IdUtil.getUniqId(), manualTrigger);
    task.setTriggerTime(triggerTime);
    return task;
  }

  class CronTaskGenerator implements Runnable {

    @Override public void run() {
      long now = System.currentTimeMillis();
      List<JobConf> jobConfs = cronTaskQueue.getNextTriggerJobs(now - TASK_TRIGGER_INTERVAL_MS * 10, now + TASK_TRIGGER_INTERVAL_MS);
      for (JobConf jobConf : jobConfs) {
        TaskConf taskConf = jobConf.getTasks().get(0);
        TriggeredTaskInfo task = generateTriggerTask(taskConf, jobConf.getJobId(),
          jobConf.getWorkerGroup(), jobConf.getNextTriggerTime(), false);
        executableTaskQueue.add(task);
        cronTaskQueue.triggeredJob(jobConf);
      }
    }
  }
}
