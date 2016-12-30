package com.dts.core.queue;

import com.dts.core.TaskGroup;
import com.dts.core.TaskInfo;
import com.dts.core.TaskStatus;
import com.dts.core.queue.kafka.KafkaCronTaskQueue;
import com.dts.core.queue.kafka.KafkaExecutableTaskQueue;
import com.dts.core.queue.kafka.KafkaExecutingTaskQueue;
import com.dts.core.queue.kafka.KafkaWorkerGroupQueue;
import com.dts.core.queue.memory.MemoryCronTaskQueue;
import com.dts.core.queue.memory.MemoryExecutableTaskQueue;
import com.dts.core.queue.memory.MemoryExecutingTaskQueue;
import com.dts.core.queue.memory.MemoryWorkerGroupQueue;
import com.dts.core.queue.mysql.MysqlCronTaskQueue;
import com.dts.core.queue.mysql.MysqlExecutableTaskQueue;
import com.dts.core.queue.mysql.MysqlExecutingTaskQueue;
import com.dts.core.queue.mysql.MysqlWorkerGroupQueue;
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
  private final ExecutingTaskQueue executingTaskQueue;
  private final TaskScheduler taskScheduler;
  private final WorkerGroupQueue workerGroupQueue;

  private final ScheduledExecutorService backend = Executors.newScheduledThreadPool(10);

  public TaskQueueContext(DTSConf conf) {
    this.conf = conf;
    String queueType = conf.get("dts.master.queue.type", "memory");
    if ("memory".equals(queueType))  {
      cronTaskQueue = new MemoryCronTaskQueue(conf);
      executableTaskQueue = new MemoryExecutableTaskQueue(conf);
      executingTaskQueue = new MemoryExecutingTaskQueue(conf);
      workerGroupQueue = new MemoryWorkerGroupQueue(conf);
    } else if ("mysql".equals(queueType)) {
      cronTaskQueue = new MysqlCronTaskQueue();
      executableTaskQueue = new MysqlExecutableTaskQueue();
      executingTaskQueue = new MysqlExecutingTaskQueue();
      workerGroupQueue = new MysqlWorkerGroupQueue();
    } else if ("kafka".equals(queueType)) {
      cronTaskQueue = new KafkaCronTaskQueue();
      executableTaskQueue = new KafkaExecutableTaskQueue();
      executingTaskQueue = new KafkaExecutingTaskQueue();
      workerGroupQueue = new KafkaWorkerGroupQueue();
    } else {
      throw new IllegalArgumentException("");
    }
    this.taskScheduler = new TaskScheduler(conf, executableTaskQueue, executingTaskQueue);
  }

  public void start() {
    backend.scheduleAtFixedRate(new CronTaskGenerator(),0L,1L, TimeUnit.SECONDS);
  }

  public synchronized TaskInfo get2ExecutingTask(String workerGroup) {
    TaskInfo task = taskScheduler.schedule(workerGroup);
    if (task != null) {
      executingTaskQueue.add(task);
      executableTaskQueue.remove(task.getId());
    }
    return task;
  }

  public synchronized TaskInfo completeTask(TaskInfo task) {
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

  public synchronized boolean removeExecutingTask(String id) {
    return executingTaskQueue.remove(id);
  }

  public boolean addCronTask(TaskInfo task) {
    return cronTaskQueue.add(task);
  }

  public boolean addTaskGroup(TaskGroup taskGroup) {
    List<TaskInfo> tasks = generateTaskFromTaskGroup(taskGroup);
    return cronTaskQueue.addBatch(tasks);
  }

  public boolean updateCronTask(TaskInfo task) {
    return cronTaskQueue.update(task);
  }

  private List<TaskInfo> generateTaskFromTaskGroup(TaskGroup taskGroup) {
    List<TaskInfo> tasks = taskGroup.getTasks();
    for (int i = 0; i < tasks.size(); ++i) {
      TaskInfo task = tasks.get(i);
      task.setCronExpression(taskGroup.getCronExpression());
      task.setWorkerGroup(taskGroup.getWorkerGroup());
      task.setGroupId(taskGroup.getTaskGroupId());
      if (i < tasks.size() - 1) {
        task.setNextTaskId(tasks.get(i + 1).getTaskId());
      }
    }
    return tasks;
  }

  public boolean updateTaskGroup(TaskGroup taskGroup) {
    List<TaskInfo> tasks = generateTaskFromTaskGroup(taskGroup);
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
      List<TaskInfo> validTasks = cronTaskQueue.getAllValid();
      for (TaskInfo task : validTasks) {
        Date nextTriggerTime = CronExpressionUtil.getNextTriggerTime(task.getCronExpression());
        Date maxTime = DateUtils.addMinutes(new Date(), conf.getInt("", 10));
        if (nextTriggerTime.before(maxTime)) {
          task.setTriggerTime(nextTriggerTime);
          executableTaskQueue.add(task);
        }
      }
    }
  }
}
