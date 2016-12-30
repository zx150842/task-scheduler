package com.dts.core.queue.memory;

import com.dts.core.TaskInfo;
import com.dts.core.queue.ExecutingTaskQueue;
import com.dts.rpc.DTSConf;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * @author zhangxin
 */
public class MemoryExecutingTaskQueue implements ExecutingTaskQueue {

  private final Map<String, TaskInfo> executingTasks = Maps.newLinkedHashMap();
  private final ListMultimap<String, TaskInfo> taskIdToTasks = ArrayListMultimap.create();
  private final DTSConf conf;

  public MemoryExecutingTaskQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override
  public boolean add(TaskInfo task) {
    TaskInfo oldTask = executingTasks.put(task.getId(), task);
    return oldTask == null;
  }

  @Override
  public boolean remove(String id) {
    TaskInfo oldTask = executingTasks.remove(id);
    return oldTask != null;
  }

  public List<TaskInfo> getByTaskId(String taskId) {
    return taskIdToTasks.get(taskId);
  }
}
