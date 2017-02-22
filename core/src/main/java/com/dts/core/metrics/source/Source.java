package com.dts.core.metrics.source;


import com.codahale.metrics.MetricRegistry;

/**
 * @author zhangxin
 */
public interface Source {
  String sourceName();
  MetricRegistry metricRegistry();
}
