package com.dts.executor;

import com.dts.core.TriggeredTaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public class TaskWrapper {
  public final TriggeredTaskInfo task;
  public final List<Object> paramValues;

  public TaskWrapper(TriggeredTaskInfo task, List<Object> paramValues) {
    this.task = task;
    this.paramValues = paramValues;
  }
}
