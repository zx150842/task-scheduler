package com.dts.core.queue.memory;

import com.dts.core.TaskInfo;
import com.dts.core.queue.ExecutableTaskQueue;
import com.dts.core.util.IdUtil;
import com.dts.rpc.DTSConf;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangxin
 */
public class MemoryExecutableTaskQueue implements ExecutableTaskQueue {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Map<String, TaskInfo> idToTasks = Maps.newConcurrentMap();
  private ListMultimap<String, TaskInfo> workerGroupToAutoTriggerTasks = ArrayListMultimap.create();
  private ListMultimap<String, TaskInfo> workerGroupToManualTriggerTasks = ArrayListMultimap.create();

  private final DTSConf conf;
  private final ReentrantLock lock = new ReentrantLock();

  public MemoryExecutableTaskQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override
  public boolean add(TaskInfo task) {
    return add(task, false);
  }

  private boolean add(TaskInfo task, boolean isResume) {
    Preconditions.checkNotNull(task);
    if (!isResume) {
      String id = IdUtil.getUniqId();
      task.setId(id);
    }
    Preconditions.checkNotNull(task.getId());
    TaskInfo oldTask;
    try {
      lock.lock();
      oldTask = idToTasks.put(task.getId(), task);
      if (task.isManualTrigger()) {
        workerGroupToManualTriggerTasks.put(task.getWorkerGroup(), task);
      } else {
        workerGroupToAutoTriggerTasks.put(task.getWorkerGroup(), task);
      }
    } finally {
      lock.unlock();
    }
    return oldTask == null;
  }

  @Override
  public boolean remove(String id) {
    TaskInfo task = idToTasks.remove(id);
    if (task != null) {
      if (task.isManualTrigger()) {
        workerGroupToManualTriggerTasks.removeAll(task.getWorkerGroup());
      } else {
        workerGroupToAutoTriggerTasks.removeAll(task.getWorkerGroup());
      }
    }
    return task != null;
  }

  @Override
  public List<TaskInfo> getManualTriggerTasks(String workerGroup) {
    return workerGroupToManualTriggerTasks.get(workerGroup);
  }

  @Override
  public List<TaskInfo> getAutoTriggerTasks(String workerGroup) {
    return workerGroupToAutoTriggerTasks.get(workerGroup);
  }

  @Override
  public TaskInfo getById(String id) {
    return idToTasks.get(id);
  }

  @Override
  public boolean resume(TaskInfo task) {
    return add(task, true);
  }
}
