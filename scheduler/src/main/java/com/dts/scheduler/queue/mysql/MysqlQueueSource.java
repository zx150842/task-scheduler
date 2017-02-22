package com.dts.scheduler.queue.mysql;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.dts.core.metrics.source.Source;

/**
 * @author zhangxin
 */
public class MysqlQueueSource implements Source {
  private static final String sourceName = "master";
  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public final Meter refreshJobMeter;

  public MysqlQueueSource() {
    this.refreshJobMeter = metricRegistry.meter(MetricRegistry.name("backend", "jobRefresh"));
  }

  @Override public String sourceName() {
    return sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
