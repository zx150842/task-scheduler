package com.dts.core.queue.memory;

import com.dts.core.queue.WorkerGroupQueue;
import com.dts.rpc.DTSConf;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * @author zhangxin
 */
public class MemoryWorkerGroupQueue implements WorkerGroupQueue {

  private final Set<String> workerGroups = Sets.newHashSet();
  private final DTSConf conf;

  public MemoryWorkerGroupQueue(DTSConf conf) {
    this.conf = conf;
  }

  @Override
  public Set<String> getAll() {
    return workerGroups;
  }
}
