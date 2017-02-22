package com.dts.core.metrics.sink;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.dts.core.DTSConf;
import com.dts.core.util.AddressUtil;
import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.InfluxdbReporter;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class InfluxDBSink implements Sink {

  private static final int INFLUXDB_DEFAULT_PERIOD = 10;
  private static final String INFLUXDB_KEY_HOST = "dts.metric.sink.influxdb.host";
  private static final String INFLUXDB_KEY_PORT = "dts.metric.sink.influxdb.port";
  private static final String INFLUXDB_KEY_USER = "dts.metric.sink.influxdb.user";
  private static final String INFLUXDB_KEY_PASSWORD = "dts.metric.sink.influxdb.password";
  private static final String INFLUXDB_KEY_DATABASE = "dts.metric.sink.influxdb.database";
  private static final String INFLUXDB_KEY_PERIOD = "dts.metric.sink.influxdb.periodSec";

  private final String host;
  private final int port;
  private final String user;
  private final String password;
  private final int pollPeriod;
  private final TimeUnit pollUnit;
  private final String database;
  private final ScheduledReporter reporter;

  public InfluxDBSink(DTSConf conf, MetricRegistry registry) {
    if (conf.get(INFLUXDB_KEY_HOST) == null) {
      throw new IllegalArgumentException("InfluxDB sink requires 'host' property.");
    }
    if (conf.get(INFLUXDB_KEY_PORT) == null) {
      throw new IllegalArgumentException("InfluxDB sink requires 'port' property.");
    }
    if (conf.get(INFLUXDB_KEY_USER) == null) {
      throw new IllegalArgumentException("InfluxDB sink requires 'user' property");
    }
    if (conf.get(INFLUXDB_KEY_PASSWORD) == null) {
      throw new IllegalArgumentException("InfluxDB sink requires 'password' property");
    }
    if (conf.get(INFLUXDB_KEY_DATABASE) == null) {
      throw new IllegalArgumentException("InfluxDB sink requires 'database' property");
    }
    this.host = conf.get(INFLUXDB_KEY_HOST);
    this.port = NumberUtils.toInt(conf.get(INFLUXDB_KEY_PORT));
    this.user = conf.get(INFLUXDB_KEY_USER);
    this.password = conf.get(INFLUXDB_KEY_PASSWORD);
    this.pollPeriod = conf.getInt(INFLUXDB_KEY_PERIOD, INFLUXDB_DEFAULT_PERIOD);
    this.pollUnit = TimeUnit.SECONDS;
    this.database = conf.get(INFLUXDB_KEY_DATABASE);

    this.reporter = InfluxdbReporter.forRegistry(registry)
      .protocol(new HttpInfluxdbProtocol("http", host, port, user, password, database))
      .convertRatesTo(pollUnit)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .filter(MetricFilter.ALL)
      .skipIdleMetrics(false)
      .tag("server", AddressUtil.getLocalHost())
      .build();
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
