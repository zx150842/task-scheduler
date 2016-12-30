package com.dts.core.queue.memory;

import com.dts.core.TaskGroup;
import com.dts.core.TaskGroupStatus;
import com.dts.core.TaskInfo;
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
  private final Map<String, TaskInfo> tasks = Maps.newConcurrentMap();
  private final DTSConf conf;

  public MemoryCronTaskQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override
  public boolean add(TaskInfo task) {
    Preconditions.checkNotNull(task);
    TaskInfo oldTask = tasks.put(task.getId(), task);
    return oldTask == null;
  }

  @Override
  public boolean update(TaskInfo task) {
    Preconditions.checkNotNull(task);
    TaskInfo oldTask = tasks.put(task.getId(), task);
    return oldTask != null;
  }

  @Override
  public boolean remove(String id) {
    TaskInfo oldTask = tasks.remove(id);
    return oldTask != null;
  }

  @Override
  public List<TaskInfo> getAllValid() {
    List<TaskInfo> validTasks = Lists.newArrayList();
    for (TaskInfo task : tasks.values()) {
      if (task.getStatus() == TaskStatus.VALID) {
        validTasks.add(task);
      }
    }
    return Collections.unmodifiableList(validTasks);
  }

  @Override public boolean addBatch(List<TaskInfo> tasks) {
    return false;
  }

  @Override public boolean updateBatch(List<TaskInfo> tasks) {
    return false;
  }

  @Override public boolean removeBatch(List<String> taskId) {
    return false;
  }
}
