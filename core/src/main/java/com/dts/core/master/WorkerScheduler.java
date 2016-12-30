package com.dts.core.master;

import com.dts.core.TaskInfo;
import com.dts.core.queue.TaskQueueContext;
import com.dts.core.util.ThreadUtils;
import com.dts.rpc.DTSConf;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author zhangxin
 */
public class WorkerScheduler {

  private final DTSConf conf;
  private final SetMultimap<String, WorkerInfo> workerGroupToWorkers = HashMultimap.create();

  public WorkerScheduler(DTSConf conf) {
    this.conf = conf;
  }

  public WorkerInfo getLaunchTaskWorker(String workerGroup) {
    // TODO add worker schedule
  }

  public boolean add(WorkerInfo worker) {
    return workerGroupToWorkers.put(worker.groupId, worker);
  }

  public boolean remove(WorkerInfo worker) {
    Set<WorkerInfo> workers = workerGroupToWorkers.get(worker.groupId);
    return workers.remove(worker);
  }
}
