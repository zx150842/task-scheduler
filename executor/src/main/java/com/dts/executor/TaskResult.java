package com.dts.executor;

import com.codahale.metrics.Timer;

/**
 * @author zhangxin
 */
public class TaskResult {
  public final Object task;
  public final Timer.Context taskContext;

  public TaskResult(Object task, Timer.Context taskContext) {
    this.task = task;
    this.taskContext = taskContext;
  }
}
