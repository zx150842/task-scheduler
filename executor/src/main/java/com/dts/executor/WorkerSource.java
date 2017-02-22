package com.dts.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.dts.core.metrics.source.Source;

/**
 * @author zhangxin
 */
public class WorkerSource implements Source {

  private static final String sourceName = "worker";
  private static final MetricRegistry metricRegistry = new MetricRegistry();

  private final Worker worker;
  // 任务从接收到上报的时间
  public final Timer taskTotalTimer;
  public final Meter taskReceiveMeter;
  public final Meter syncMasterMeter;

  public WorkerSource(Worker worker) {
    this.worker = worker;
    this.taskTotalTimer = metricRegistry.timer(MetricRegistry.name("task", "totalTime"));
    this.taskReceiveMeter = metricRegistry.meter(MetricRegistry.name("task", "receives"));
    this.syncMasterMeter = metricRegistry.meter(MetricRegistry.name("backend", "masterSync"));

    metricRegistry.register("leader", (Gauge<String>)() -> worker.master.orElse(worker.UNKNOWN_MASTER).address().hostPort);
  }

  @Override public String sourceName() {
    return this.sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return this.metricRegistry;
  }
}
