package com.dts.core.metrics;

import com.codahale.metrics.MetricRegistry;
import com.dts.core.DTSConf;
import com.dts.core.metrics.sink.InfluxDBSink;
import com.dts.core.metrics.sink.Sink;
import com.dts.core.metrics.source.JvmSource;
import com.dts.core.metrics.source.Source;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhangxin
 */
public class MetricsSystem {
  private final Logger logger = LoggerFactory.getLogger(MetricsSystem.class);

  public final DTSConf conf;

  private static MetricsSystem metricsSystem;
  private final List<Sink> sinks = Lists.newArrayList();
  private final List<Source> sources = Lists.newArrayList();
  public final MetricRegistry registry = new MetricRegistry();

  private boolean running;

  private MetricsSystem(DTSConf conf) {
    this.conf = conf;
    init();
  }

  private void init() {
    registerSource(new JvmSource());
    registerSink(new InfluxDBSink(conf, registry));
  }

  public static MetricsSystem createMetricsSystem(DTSConf conf) {
    if (metricsSystem == null) {
      synchronized (MetricsSystem.class) {
        if (metricsSystem == null) {
          metricsSystem = new MetricsSystem(conf);
        }
      }
    }
    return metricsSystem;
  }

  public void start() {
    Preconditions.checkArgument(!running, "Attempting to start a MetricsSystem that is already running");
    running = true;
    sinks.forEach(sink -> sink.start());
  }

  public void stop() {
    if (running) {
      sinks.forEach(sink -> sink.stop());
    } else {
      logger.warn("Stopping a MetricsSystem that is not running");
    }
    running = false;
  }

  public void report() {
    sinks.forEach(sink -> sink.report());
  }

  public void registerSource(Source source) {
    sources.add(source);
    try {
      String regName = buildRegistryName(source);
      registry.register(regName, source.metricRegistry());
    } catch (IllegalArgumentException e) {
      logger.info("Metrics already registered", e);
    }
  }

  private String buildRegistryName(Source source) {
    String defaultName = MetricRegistry.name(source.sourceName());
    return defaultName;
  }

  public void registerSink(Sink sink) {
    sinks.add(sink);
  }
}
