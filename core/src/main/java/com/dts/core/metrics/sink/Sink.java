package com.dts.core.metrics.sink;

/**
 * @author zhangxin
 */
public interface Sink {
  void start();
  void stop();
  void report();
}
