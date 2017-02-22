package com.dts.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.dts.core.metrics.source.Source;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zhangxin
 */
public class TaskDispatcherSource implements Source {
  private static final String sourceName = "worker";
  private static final MetricRegistry metricRegistry = new MetricRegistry();

  public TaskDispatcherSource(TaskDispatcher taskDispatcher) {
    ThreadPoolExecutor taskThreadPool = taskDispatcher.threadpool();
    metricRegistry.register(MetricRegistry.name("threadpool", "activeTasks"), (Gauge<Integer>)() -> taskThreadPool.getActiveCount());
    metricRegistry.register(MetricRegistry.name("threadpool", "completeTasks"), (Gauge<Long>)() ->taskThreadPool.getCompletedTaskCount());
    metricRegistry.register(MetricRegistry.name("threadpool", "currentPoolSize"), (Gauge<Integer>)() -> taskThreadPool.getPoolSize());
    metricRegistry.register(MetricRegistry.name("threadpool", "maxPoolSize"), (Gauge<Integer>)() -> taskThreadPool.getMaximumPoolSize());
  }

  @Override public String sourceName() {
    return sourceName;
  }

  @Override public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
