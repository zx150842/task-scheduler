package com.dts.scheduler;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.dts.core.metrics.source.Source;

/**
 * @author zhangxin
 */
public class MasterSource implements Source {
  private static final String sourceName = "master";
  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public final Meter successTaskMeter;
  public final Meter failTaskMeter;
  public final Meter resumeTaskMeter;
  public final Meter syncWorkerMeter;
  public final Meter sendTaskMeter;

  public MasterSource(Master master) {
    this.successTaskMeter = metricRegistry.meter(MetricRegistry.name("task", "successes"));
    this.failTaskMeter = metricRegistry.meter(MetricRegistry.name("task", "fails"));
    this.resumeTaskMeter = metricRegistry.meter(MetricRegistry.name("task", "resumes"));
    this.syncWorkerMeter = metricRegistry.meter(MetricRegistry.name("backend", "workerSync"));
    this.sendTaskMeter = metricRegistry.meter(MetricRegistry.name("task", "sends"));

    metricRegistry.register("state",
      (Gauge<Integer>)() -> master.state == RecoveryState.ALIVE ? 1 : 0);
    metricRegistry.register("validWorkers",
      (Gauge<Integer>)() -> master.workerGroups().values().size());
    metricRegistry.register(MetricRegistry.name("task", "executables"),
      (Gauge<Integer>)() -> master.taskQueueContext.executableTaskCache.size());
    metricRegistry.register(MetricRegistry.name("task", "executings"),
      (Gauge<Integer>)() -> master.taskQueueContext.executingTaskCache.values().size());
  }

  @Override public String sourceName() {
    return sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
