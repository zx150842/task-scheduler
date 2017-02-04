package com.dts.executor;

import com.google.common.base.Throwables;

import com.dts.core.DeployMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class TaskRunner implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public final Worker worker;
  public final TaskWrapper tw;
  public final Method method;
  public final Object instance;

  public TaskRunner(Worker worker, TaskWrapper tw, Method method, Object instance) {
    this.worker = worker;
    this.tw = tw;
    this.method = method;
    this.instance = instance;
  }

  @Override public void run() {
    List<Object> params = tw.paramValues;
    Object[] args = null;
    if (params != null && !params.isEmpty()) {
      args = params.toArray(new Object[] {});
    }
    try {
      String threadName = Thread.currentThread().getName();
      worker.addToReportQueue(new ExecutingTask(tw.task, threadName));
      method.invoke(instance, args);
      worker.addToReportQueue(new FinishTask(tw.task, "success"));
    } catch (Throwable e) {
      worker.addToReportQueue(new FinishTask(tw.task, Throwables.getStackTraceAsString(e)));
      logger.error("Invoke method {} of task {} failed", method, tw.task, e);
    }
  }
}
