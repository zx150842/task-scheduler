package com.dts.core.metrics.sink;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.GraphiteUDP;
import com.dts.core.DTSConf;
import com.dts.core.exception.DTSException;
import org.apache.commons.lang3.math.NumberUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class GraphiteSink implements Sink {

  private static final int GRAPHITE_DEFAULT_PERIOD = 10;
  private static final String GRAPHITE_DEFAULT_PREFIX = "";
  private static final String GRAPHITE_KEY_HOST = "dts.metric.sink.graphite.host";
  private static final String GRAPHITE_KEY_PORT = "dts.metric.sink.graphite.port";
  private static final String GRAPHITE_KEY_PERIOD = "dts.metric.sink.graphite.periodSec";
  private static final String GRAPHITE_KEY_PREFIX = "dts.metric.sink.graphite.prefix";
  private static final String GRAPHITE_KEY_PROTOCOL = "dts.metric.sink.graphite.protocol";

  private final String host;
  private final int port;
  private final int pollPeriod;
  private final TimeUnit pollUnit;
  private final String prefix;
  private final GraphiteSender graphite;
  private final GraphiteReporter reporter;

  public GraphiteSink(DTSConf conf, MetricRegistry registry) {

    if (conf.get(GRAPHITE_KEY_HOST) == null) {
      throw new IllegalArgumentException("Graphite sink requires 'host' property.");
    }
    if (conf.get(GRAPHITE_KEY_PORT) == null) {
      throw new IllegalArgumentException("Graphite sink requires 'port' property.");
    }
    this.host = conf.get(GRAPHITE_KEY_HOST);
    this.port = NumberUtils.toInt(conf.get(GRAPHITE_KEY_PORT));
    this.pollPeriod = conf.getInt(GRAPHITE_KEY_PERIOD, GRAPHITE_DEFAULT_PERIOD);
    this.pollUnit = TimeUnit.SECONDS;
    this.prefix = conf.get(GRAPHITE_KEY_PREFIX, GRAPHITE_DEFAULT_PREFIX);

    String protocol = conf.get(GRAPHITE_KEY_PROTOCOL);
    if ("udp".equals(protocol)) {
      this.graphite = new GraphiteUDP(new InetSocketAddress(host, port));
    } else if (protocol == null || "tcp".equals(protocol)) {
      this.graphite = new Graphite(new InetSocketAddress(host, port));
    } else {
      throw new DTSException("Invalid Graphite protocol: " + protocol);
    }

    this.reporter = GraphiteReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .prefixedWith(prefix)
      .build(graphite);
  }

  @Override public void start() {
    reporter.start(pollPeriod, pollUnit);
  }

  @Override public void stop() {
    reporter.stop();
  }

  @Override public void report() {
    reporter.report();
  }
}
