package com.dts.executor;

import com.codahale.metrics.Timer;
import com.dts.core.TriggeredTaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public class TaskWrapper {
  public final TriggeredTaskInfo task;
  public final List<Object> paramValues;
  public final Timer.Context timerContext;

  public TaskWrapper(TriggeredTaskInfo task, List<Object> paramValues, Timer.Context timerContext) {
    this.task = task;
    this.paramValues = paramValues;
    this.timerContext = timerContext;
  }
}
