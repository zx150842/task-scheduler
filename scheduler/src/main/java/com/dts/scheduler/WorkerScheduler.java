package com.dts.scheduler;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * worker调度器，根据调度策略选择一台worker来执行任务
 * <p>当前只实现了随机选择策略</p>
 *
 * @author zhangxin
 */
public class WorkerScheduler {
  private final Logger logger = LoggerFactory.getLogger(WorkerScheduler.class);

  private final Master master;
  private final Random RANDOM = new Random();

  public WorkerScheduler(Master master) {
    this.master = master;
  }

  public WorkerInfo getLaunchTaskWorker(String workerGroup, boolean runOnSeed) {
    List<WorkerInfo> workers;
    if (master.workerGroups() == null || master.workerGroups().isEmpty()) {
      logger.error("There is no online workers in {}", workerGroup);
      return null;
    }
    workers = master.workerGroups().get(workerGroup);
    if (workers == null || workers.isEmpty()) {
      logger.error("There is no workers in {}", workerGroup);
      return null;
    }
    List<WorkerInfo> aliveWorkers = Lists.newArrayList();
    for (WorkerInfo worker : workers) {
      if (worker.isAlive() && !(worker.isSeed ^ runOnSeed)) {
        aliveWorkers.add(worker);
      }
    }
    if (aliveWorkers.isEmpty()) {
      logger.error("There is no alive workers in {}", workerGroup);
      return null;
    }
    int workerIdx = RANDOM.nextInt(aliveWorkers.size());
    return aliveWorkers.get(workerIdx);
  }
}
