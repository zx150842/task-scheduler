package com.dts.core.queue.memory;

import com.dts.core.TaskConf;
import com.dts.core.TaskConf;
import com.dts.core.TaskStatus;
import com.dts.core.queue.CronTaskQueue;
import com.dts.rpc.DTSConf;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author zhangxin
 */
public class MemoryCronTaskQueue implements CronTaskQueue {

  // task id -> task
  private final Map<String, TaskConf> taskMap = Maps.newConcurrentMap();
  private final Map<String, List<TaskConf>> taskGroupMap = Maps.newConcurrentMap();
  private final DTSConf conf;

  public MemoryCronTaskQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override
  public boolean add(TaskConf task) {
    Preconditions.checkNotNull(task);
    TaskConf oldTask = taskMap.put(task.getTaskId(), task);
    return oldTask == null;
  }

  @Override
  public boolean update(TaskConf task) {
    Preconditions.checkNotNull(task);
    TaskConf oldTask = taskMap.put(task.getTaskId(), task);
    return oldTask != null;
  }

  @Override
  public boolean remove(String id) {
    TaskConf oldTask = taskMap.remove(id);
    return oldTask != null;
  }

  @Override
  public List<TaskConf> getAllValid() {
    List<TaskConf> validTasks = Lists.newArrayList();
    for (TaskConf task : taskMap.values()) {
      if (task.getStatus() == TaskStatus.VALID) {
        validTasks.add(task);
      }
    }
    return Collections.unmodifiableList(validTasks);
  }

  @Override public boolean addBatch(List<TaskConf> tasks) {
    if (tasks == null) {
      return false;
    }
    for (TaskConf task : tasks) {
      add(task);
    }
    taskGroupMap.put(tasks.get(0).getTaskGroup(), tasks);
    return true;
  }

  @Override public boolean updateBatch(List<TaskConf> tasks) {
    if (tasks == null) {
      return false;
    }
    for (TaskConf task : tasks) {
      update(task);
    }
    taskGroupMap.put(tasks.get(0).getTaskGroup(), tasks);
    return true;
  }

  @Override public boolean removeBatch(List<String> taskIds) {
    if (taskIds == null) {
      return false;
    }
    String groupId = null;
    for (String taskId : taskIds) {
      TaskConf task = taskMap.get(taskId);
      if (task != null) {
        String taskGroupId = task.getTaskGroup();
        if (groupId == null) {
          groupId = taskGroupId;
        }
      }
      remove(taskId);
    }
    if (groupId != null) {
      taskGroupMap.remove(groupId);
    }
    return false;
  }

  @Override public List<String> getTaskId(String taskGroupId) {
    List<TaskConf> tasks = getTasks(taskGroupId);
    List<String> taskIds = Lists.newArrayList();
    for (TaskConf task : tasks) {
      taskIds.add(task.getTaskId());
    }
    return taskIds;
  }

  public TaskConf getTask(String taskId) {
    return taskMap.get(taskId);
  }

  public List<TaskConf> getTasks(String taskGroupId) {
    return taskGroupMap.get(taskGroupId);
  }

  public boolean containTask(String taskId) {
    return taskMap != null && taskMap.containsKey(taskId);
  }

  public boolean containTaskGroup(String taskGroupId) {
    return taskGroupMap != null && taskGroupMap.containsKey(taskGroupId);
  }
}
