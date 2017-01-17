package com.dts.core.queue.memory;

import com.dts.core.TaskInfo;
import com.dts.core.queue.LaunchedTaskQueue;
import com.dts.rpc.DTSConf;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author zhangxin
 */
public class MemoryLaunchedTaskQueue implements LaunchedTaskQueue {

  private final Map<String, TaskInfo> launchedTasks = Maps.newConcurrentMap();
  private final DTSConf conf;

  public MemoryLaunchedTaskQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override public boolean add(TaskInfo task) {
    TaskInfo oldTask = launchedTasks.put(task.getId(), task);
    return oldTask == null;
  }

  @Override public boolean remove(String id) {
    TaskInfo oldTask = launchedTasks.remove(id);
    return oldTask != null;
  }
}
