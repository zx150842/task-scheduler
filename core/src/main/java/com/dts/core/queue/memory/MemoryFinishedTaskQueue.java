package com.dts.core.queue.memory;

import com.dts.core.TaskInfo;
import com.dts.core.queue.FinishedTaskQueue;
import com.dts.rpc.DTSConf;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author zhangxin
 */
public class MemoryFinishedTaskQueue implements FinishedTaskQueue {

  private final Map<String, TaskInfo> finishedTasks = Maps.newConcurrentMap();
  private final DTSConf conf;

  public MemoryFinishedTaskQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override public boolean add(TaskInfo task) {
    TaskInfo oldTask = finishedTasks.put(task.getId(), task);
    return oldTask == null;
  }
}
