package com.dts.executor;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.dts.core.metrics.source.Source;

/**
 * @author zhangxin
 */
public class TaskRunnerSource implements Source {
  private static final String sourceName = "worker";
  private static final MetricRegistry metricRegistry = new MetricRegistry();

  // 任务运行时间
  public final Timer taskExecuteTimer;

  public TaskRunnerSource() {
    this.taskExecuteTimer = metricRegistry.timer(MetricRegistry.name("task", "executeTime"));
  }

  @Override public String sourceName() {
    return sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
