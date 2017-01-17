package com.dts.core.queue;

import com.dts.core.TaskConf;
import com.dts.core.TaskGroup;
import com.dts.core.TaskInfo;
import com.dts.core.TaskStatus;
import com.dts.core.queue.kafka.*;
import com.dts.core.queue.memory.*;
import com.dts.core.queue.mysql.*;
import com.dts.core.util.CronExpressionUtil;
import com.dts.rpc.DTSConf;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class TaskQueueContext {

  private final DTSConf conf;
  private final CronTaskQueue cronTaskQueue;
  private final ExecutableTaskQueue executableTaskQueue;
  private final LaunchingTaskQueue launchingTaskQueue;
  private final LaunchedTaskQueue launchedTaskQueue;
  private final ExecutingTaskQueue executingTaskQueue;
  private final FinishedTaskQueue finishedTaskQueue;
  private final TaskScheduler taskScheduler;
  private final WorkerGroupQueue workerGroupQueue;

  private final ScheduledExecutorService backend = Executors.newScheduledThreadPool(10);

  public TaskQueueContext(DTSConf conf) {
    this.conf = conf;
    String queueType = conf.get("dts.master.queue.type", "memory");
    if ("memory".equals(queueType))  {
      cronTaskQueue = new MemoryCronTaskQueue(conf);
      executableTaskQueue = new MemoryExecutableTaskQueue(conf);
      launchingTaskQueue = new MemoryLaunchingTaskQueue(conf);
      launchedTaskQueue = new MemoryLaunchedTaskQueue(conf);
      executingTaskQueue = new MemoryExecutingTaskQueue(conf);
      finishedTaskQueue = new MemoryFinishedTaskQueue(conf);
      workerGroupQueue = new MemoryWorkerGroupQueue(conf);
    } else if ("mysql".equals(queueType)) {
      cronTaskQueue = new MysqlCronTaskQueue();
      executableTaskQueue = new MysqlExecutableTaskQueue();
      launchingTaskQueue = new MysqlLaunchingTaskQueue();
      launchedTaskQueue = new MysqlLaunchedTaskQueue();
      executingTaskQueue = new MysqlExecutingTaskQueue();
      finishedTaskQueue = new MysqlFinishedTaskQueue();
      workerGroupQueue = new MysqlWorkerGroupQueue();
    } else if ("kafka".equals(queueType)) {
      cronTaskQueue = new KafkaCronTaskQueue();
      executableTaskQueue = new KafkaExecutableTaskQueue();
      launchingTaskQueue = new KafkaLaunchingTaskQueue();
      launchedTaskQueue = new KafkaLaunchedTaskQueue();
      executingTaskQueue = new KafkaExecutingTaskQueue();
      finishedTaskQueue = new KafkaFinishedTaskQueue();
      workerGroupQueue = new KafkaWorkerGroupQueue();
    } else {
      throw new IllegalArgumentException("");
    }
    this.taskScheduler = new TaskScheduler(conf, executableTaskQueue, launchingTaskQueue);
  }

  public void start() {
    backend.scheduleAtFixedRate(new CronTaskGenerator(),0L,1L, TimeUnit.SECONDS);
  }

  public synchronized TaskInfo get2LaunchingTask(String workerGroup) {
    TaskInfo task = taskScheduler.schedule(workerGroup);
    if (task != null) {
      launchingTaskQueue.add(task);
      executableTaskQueue.remove(task.getId());
    }
    return task;
  }

  public synchronized boolean launchedTask(TaskInfo task) {
    boolean success;
    if (task != null) {
      success = launchedTaskQueue.add(task);
      if (success) {
        success = launchingTaskQueue.remove(task.getId());
      }
    } else {
      success = false;
    }
    return success;
  }

  public synchronized boolean executingTask(TaskInfo task) {
    boolean success;
    if (task != null) {
      success = executingTaskQueue().add(task);
      if (success) {
        launchedTaskQueue.remove(task.getId());
      }
    } else {
      success = false;
    }
    return success;
  }

  public synchronized TaskInfo completeTask(TaskInfo task) {
    finishedTaskQueue.add(task);
    executingTaskQueue.remove(task.getId());
    if (task.getStatus() != TaskStatus.SUCCESS) {
      executableTaskQueue.resume(task);
      return null;
    }
    if (task.getNextId() != null) {
      TaskInfo nextTask = executableTaskQueue.getById(task.getNextId());
      executingTaskQueue.add(nextTask);
      executableTaskQueue.remove(nextTask.getId());
      return nextTask;
    }
    return null;
  }

  public synchronized boolean addNextTaskToExecutableQueue(TaskInfo task) {
    return executableTaskQueue.add(task);
  }

  public synchronized boolean triggerTask(String taskId) {
    if (cronTaskQueue.containTask(taskId)) {
      TaskConf taskConf = cronTaskQueue.getTask(taskId);
      TaskInfo task = new TaskInfo(taskConf);
      task.setManualTrigger(true);
      return executableTaskQueue.add(task);
    }
    return false;
  }

  public synchronized boolean triggerTaskGroup(String taskGroupId) {
    if (cronTaskQueue.containTaskGroup(taskGroupId)) {
      List<TaskConf> taskConfs = cronTaskQueue.getTasks(taskGroupId);
      for (TaskConf taskConf : taskConfs) {
        TaskInfo task = new TaskInfo(taskConf);
        task.setManualTrigger(true);
        executableTaskQueue.add(task);
      }
      return true;
    }
    return false;
  }

  public boolean addCronTask(TaskConf task) {
    return cronTaskQueue.add(task);
  }

  public boolean addTaskGroup(TaskGroup taskGroup) {
    List<TaskConf> tasks = generateTaskFromTaskGroup(taskGroup);
    return cronTaskQueue.addBatch(tasks);
  }

  public boolean updateCronTask(TaskConf taskConf) {
    return cronTaskQueue.update(taskConf);
  }

  private List<TaskConf> generateTaskFromTaskGroup(TaskGroup taskGroup) {
    List<TaskConf> taskConfs = taskGroup.getTasks();
    for (int i = 0; i < taskConfs.size(); ++i) {
      TaskConf task = taskConfs.get(i);
      task.setCronExpression(taskGroup.getCronExpression());
      task.setWorkerGroup(taskGroup.getWorkerGroup());
      task.setTaskGroup(taskGroup.getTaskGroupId());
      if (i < taskConfs.size() - 1) {
        task.setNextTaskId(taskConfs.get(i + 1).getTaskId());
      }
    }
    return taskConfs;
  }

  public boolean updateTaskGroup(TaskGroup taskGroup) {
    List<TaskConf> tasks = generateTaskFromTaskGroup(taskGroup);
    return cronTaskQueue.updateBatch(tasks);
  }

  public boolean removeCronTask(String taskId) {
    return cronTaskQueue.remove(taskId);
  }

  public boolean removeTaskGroup(String taskGroupId) {
    List<String> taskIds = cronTaskQueue.getTaskId(taskGroupId);
    return cronTaskQueue.removeBatch(taskIds);
  }

  public CronTaskQueue cronTaskQueue() {
    return cronTaskQueue;
  }

  public ExecutableTaskQueue executableTaskQueue() {
    return executableTaskQueue;
  }

  public ExecutingTaskQueue executingTaskQueue() {
    return executingTaskQueue;
  }

  public DTSConf getConf() {
    return conf;
  }

  public Set<String> getWorkerGroups() {
    return workerGroupQueue.getAll();
  }

  class CronTaskGenerator implements Runnable {

    @Override public void run() {
      List<TaskConf> validTaskConfs = cronTaskQueue.getAllValid();
      for (TaskConf taskConf : validTaskConfs) {
        Date nextTriggerTime = CronExpressionUtil.getNextTriggerTime(taskConf.getCronExpression());
        Date maxTime = DateUtils.addMinutes(new Date(), conf.getInt("dts.task.trigger.interval", 10));
        if (nextTriggerTime.before(maxTime)) {
          taskConf.setTriggerTime(nextTriggerTime);
          TaskInfo task = new TaskInfo(taskConf);
          executableTaskQueue.add(task);
        }
      }
    }
  }
}
