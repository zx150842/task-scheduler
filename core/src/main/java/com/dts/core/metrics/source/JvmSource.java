package com.dts.core.metrics.source;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

/**
 * @author zhangxin
 */
public class JvmSource implements Source {

  private String sourceName = "jvm";
  private MetricRegistry registry = new MetricRegistry();

  public JvmSource() {
    registry.registerAll(new GarbageCollectorMetricSet());
    registry.registerAll(new MemoryUsageGaugeSet());
  }

  @Override public String sourceName() {
    return this.sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return registry;
  }
}
