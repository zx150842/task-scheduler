package com.dts.scheduler.queue;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.dts.core.metrics.source.Source;

/**
 * @author zhangxin
 */
public class TaskQueueSource implements Source {
  private static final String sourceName = "master";
  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public final Meter timeoutTaskMeter;
  public final Meter skipTaskMeter;
  public final Meter timeoutTaskCheckMeter;
  public final Meter taskGenerateMeter;

  public TaskQueueSource() {
    this.timeoutTaskMeter = metricRegistry.meter(MetricRegistry.name("task", "timeouts"));
    this.skipTaskMeter = metricRegistry.meter(MetricRegistry.name("task", "skips"));
    this.timeoutTaskCheckMeter = metricRegistry.meter(MetricRegistry.name("backend", "taskTimeoutCheck"));
    this.taskGenerateMeter = metricRegistry.meter(MetricRegistry.name("backend", "taskGenerate"));
  }

  @Override public String sourceName() {
    return sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
