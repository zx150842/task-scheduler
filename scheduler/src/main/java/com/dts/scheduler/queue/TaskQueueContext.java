package com.dts.scheduler.queue;

import com.dts.core.*;
import com.dts.core.metrics.MetricsSystem;
import com.dts.core.util.CronExpressionUtil;
import com.dts.core.util.IdUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.scheduler.MasterSource;
import com.dts.scheduler.queue.mysql.MysqlFinishedTaskQueue;

import com.google.common.base.Throwables;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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

  private final CronTaskQueue cronTaskQueue;
  private final ExecutableTaskQueue executableTaskQueue;
  private final ExecutingTaskQueue executingTaskQueue;
  private final FinishedTaskQueue finishedTaskQueue;
  private final TaskScheduler taskScheduler;

  private final ScheduledExecutorService generateExecutableTaskThread;
  private final ScheduledExecutorService executingTaskTimeoutCheckThread;

  private final long TASK_TRIGGER_INTERVAL_MS;
  private final long TASK_TIMEOUT_CHECK_MS;
  private final long TASK_MAX_EXECUTING_TIME_MS;

  public final LinkedBlockingDeque<TriggeredTaskInfo> executableTaskCache =
      Queues.newLinkedBlockingDeque();
  public final Map<String, String> lastExecuteTaskSysIdCache = Maps.newHashMap();
  public final ListMultimap<String, TriggeredTaskInfo> executingTaskCache =
      ArrayListMultimap.create();

  public final TaskQueueSource taskQueueSource;

  public TaskQueueContext(DTSConf conf) {
    TASK_TRIGGER_INTERVAL_MS = conf.getLong("dts.task.trigger.intervalMs", 1000);
    TASK_TIMEOUT_CHECK_MS = conf.getLong("dts.task.timeout.checkMs", 1000);
    TASK_MAX_EXECUTING_TIME_MS = conf.getLong("dts.task.max.executingSec", 3600) * 1000;
    String queueType = conf.get("dts.task.queue.type", "mysql");
    if ("mysql".equals(queueType)) {
      cronTaskQueue = new MysqlCronTaskQueue(conf);
      executableTaskQueue = new MysqlExecutableTaskQueue();
      executingTaskQueue = new MysqlExecutingTaskQueue();
      finishedTaskQueue = new MysqlFinishedTaskQueue();
    } else {
      throw new IllegalArgumentException("Not Support 'dts.task.queue.type'= " + queueType);
    }
    this.taskQueueSource = new TaskQueueSource();
    MetricsSystem.createMetricsSystem(conf).registerSource(taskQueueSource);
    this.taskScheduler = new TaskScheduler(conf, this);
    this.generateExecutableTaskThread =
        ThreadUtil.newDaemonSingleThreadScheduledExecutor("task-generator");
    this.executingTaskTimeoutCheckThread =
        ThreadUtil.newDaemonSingleThreadScheduledExecutor("timeout-executing-task-remove");
    initCache();
  }

  private void initCache() {
    List<TriggeredTaskInfo> executableTasks = executableTaskQueue.getAll();
    Set<String> taskIdSet = Sets.newHashSet();
    for (TriggeredTaskInfo task : executableTasks) {
      if (!taskIdSet.contains(task.getTaskId())) {
        executableTaskCache.offer(task);
        taskIdSet.add(task.getTaskId());
      }
    }
    // TODO add executing tasks
  }

  public void start() {
    generateExecutableTaskThread.scheduleAtFixedRate(new CronTaskGenerator(), 0L,
        TASK_TRIGGER_INTERVAL_MS / 2, TimeUnit.MILLISECONDS);
    executingTaskTimeoutCheckThread.scheduleAtFixedRate(new TaskTimeOutCheck(), 0L,
        TASK_TIMEOUT_CHECK_MS, TimeUnit.MILLISECONDS);
  }

  public synchronized TriggeredTaskInfo get2ExecutingTask() throws InterruptedException {
    return taskScheduler.schedule();
  }

  public synchronized boolean resumeTask(TriggeredTaskInfo task) {
    return executableTaskCache.offer(task);
  }

  public synchronized boolean skipTask(TriggeredTaskInfo task) {
    taskQueueSource.skipTaskMeter.mark();
    boolean success;
    success = executableTaskQueue.remove(task.getSysId());
    if (success) {
      task.setStatus(TaskStatus.SKIP);
      success = finishedTaskQueue.add(task);
    }
    return success;
  }

  public synchronized boolean executingTask(TriggeredTaskInfo task) {
    boolean success;
    if (task != null) {
      success = executingTaskQueue.add(task);
      if (!task.isManualTrigger()) {
        task.setExecutingTime(new Timestamp(System.currentTimeMillis()));
        lastExecuteTaskSysIdCache.put(task.getTaskId(), task.getSysId());
        executingTaskCache.put(task.getTaskId(), task);
      }
    } else {
      success = false;
    }
    return success;
  }

  public synchronized void completeTask(String sysId, String message) {
    TriggeredTaskInfo task = executingTaskQueue.getBySysId(sysId);
    if (task == null) {
      logger.error("executing queue has no task, sysId: {}", sysId);
      return;
    }
    task.setExecuteResult(message);
    if ("success".equals(message)) {
      task.setStatus(TaskStatus.SUCCESS);
    } else {
      task.setStatus(TaskStatus.FAIL);
    }
    finishedTaskQueue.add(task);
    executingTaskQueue.remove(task.getSysId());
    removeExecutingTask(sysId, task.getTaskId());
    TaskConf nextTaskConf = cronTaskQueue.getNextToTriggerTask(task.getJobId(), task.getTaskName());
    if (nextTaskConf != null) {
      TriggeredTaskInfo nextTask = generateTriggerTask(nextTaskConf, task.getJobId(),
          task.getWorkerGroup(), new Date(), false, null);
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
      TriggeredTaskInfo task =
          generateTriggerTask(taskConf, jobId, jobConf.getWorkerGroup(), new Date(), true, null);
      executableTaskQueue.add(task);
      executableTaskCache.offerFirst(task);
      return true;
    }
    return false;
  }

  private synchronized void removeExecutingTask(String sysId, String taskId) {
    List<TriggeredTaskInfo> executingTasks = executingTaskCache.get(taskId);
    if (executingTasks != null) {
      TriggeredTaskInfo toRemoveTask = null;
      for (TriggeredTaskInfo executingTask : executingTasks) {
        if (executingTask.getSysId().equals(sysId)) {
          toRemoveTask = executingTask;
          break;
        }
      }
      if (toRemoveTask != null) {
        executingTaskCache.remove(taskId, toRemoveTask);
      }
    }
  }

  private TriggeredTaskInfo generateTriggerTask(TaskConf taskConf, String jobId, String workerGroup,
      Date triggerTime, boolean manualTrigger, String cronExpression) {
    TriggeredTaskInfo task = new TriggeredTaskInfo(jobId, workerGroup, taskConf.getTaskId(),
        taskConf.getTaskName(), taskConf.getParams(), IdUtil.getUniqId(), manualTrigger);
    task.setTriggerTime(triggerTime);
    if (cronExpression != null) {
      Date nextTriggerTime = CronExpressionUtil.getNextTriggerTime(cronExpression, triggerTime);
      task.setNextTriggerTime(nextTriggerTime);
    }
    return task;
  }

  /**
   * 定时根据cron生成可执行任务
   */
  class CronTaskGenerator implements Runnable {

    @Override
    public void run() {
      taskQueueSource.taskGenerateMeter.mark();
      ReentrantLock triggerLock = cronTaskQueue.triggerJobLock();
      try {
        if (triggerLock.tryLock(100, TimeUnit.MILLISECONDS)) {
          long now = System.currentTimeMillis();
          List<JobConf> jobConfs = cronTaskQueue.getNextTriggerJobs(
              now - TASK_TRIGGER_INTERVAL_MS * 10, now + TASK_TRIGGER_INTERVAL_MS);
          for (JobConf jobConf : jobConfs) {
            TaskConf taskConf = jobConf.getTasks().get(0);
            TriggeredTaskInfo task =
                generateTriggerTask(taskConf, jobConf.getJobId(), jobConf.getWorkerGroup(),
                    jobConf.getNextTriggerTime(), false, jobConf.getCronExpression());
            // 将可执行任务持久化到队列中
            executableTaskQueue.add(task);
            // 将可执行任务缓存到内存中
            executableTaskCache.offer(task);
            // 更新任务的触发时间和下次触发时间
            cronTaskQueue.triggeredJob(jobConf);
          }
        } else {
          logger.warn(
              "Cannot acquire trigger lock for waiting 100 ms, ignore this cronTaskGenerator");
        }
      } catch (InterruptedException e) {
        logger.error(Throwables.getStackTraceAsString(e));
      } finally {
        if (triggerLock.isHeldByCurrentThread()) {
          triggerLock.unlock();
        }
      }
    }
  }

  class TaskTimeOutCheck implements Runnable {

    @Override
    public void run() {
      taskQueueSource.timeoutTaskCheckMeter.mark();
      List<TriggeredTaskInfo> tasks = (List<TriggeredTaskInfo>) executingTaskCache.values();
      if (tasks == null || tasks.isEmpty()) {
        return;
      }
      long now = System.currentTimeMillis();
      for (TriggeredTaskInfo task : tasks) {
        if (task.getExecutingTime() == null) {
          logger.error("Executing Task [sysId={}], executingTime is null", task.getSysId());
          continue;
        }
        long executingTimeMs = task.getExecutingTime().getTime() - now;
        long nextTriggerTime = task.getNextTriggerTime() != null
            ? task.getNextTriggerTime().getTime() : Long.MAX_VALUE;
        long maxExecutingTimeMs =
            Math.min(TASK_MAX_EXECUTING_TIME_MS, nextTriggerTime - task.getTriggerTime().getTime());
        if (maxExecutingTimeMs <= executingTimeMs) {
          executingTaskQueue.resume(task.getSysId());
          removeExecutingTask(task.getSysId(), task.getTaskId());
          executableTaskCache.add(task);
          taskQueueSource.timeoutTaskMeter.mark();
          logger.warn(
              "Task [sysId={}] has already run {} ms, greater than max run time {}, resume task to executable",
              task.getSysId(), executingTimeMs, maxExecutingTimeMs);
        }
      }
    }
  }
}
