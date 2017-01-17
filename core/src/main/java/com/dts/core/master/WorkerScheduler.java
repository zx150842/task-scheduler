package com.dts.core.master;

import com.dts.rpc.DTSConf;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author zhangxin
 */
public class WorkerScheduler {

  private final DTSConf conf;
  private final ListMultimap<String, WorkerInfo> workerGroupToWorkers = ArrayListMultimap.create();

  public WorkerScheduler(DTSConf conf) {
    this.conf = conf;
  }

  public WorkerInfo getLaunchTaskWorker(String workerGroup) {
    List<WorkerInfo> workers = workerGroupToWorkers.get(workerGroup);
    Random random = new Random();
    int workerIdx = random.nextInt(workers.size());
    return workers.get(workerIdx);
  }

  public boolean add(WorkerInfo worker) {
    return workerGroupToWorkers.put(worker.groupId, worker);
  }

  public boolean remove(WorkerInfo worker) {
    List<WorkerInfo> workers = workerGroupToWorkers.get(worker.groupId);
    return workers.remove(worker);
  }
}
