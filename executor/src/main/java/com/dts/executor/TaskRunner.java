package com.dts.executor;

import com.codahale.metrics.Timer;
import com.dts.core.DTSConf;

import com.google.common.base.Throwables;

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
//  private final TaskRunnerSource taskRunnerSource;

  public TaskRunner(Worker worker, DTSConf conf, TaskWrapper tw, Method method, Object instance) {
    this.worker = worker;
    this.tw = tw;
    this.method = method;
    this.instance = instance;
//    this.taskRunnerSource = new TaskRunnerSource();
//    MetricsSystem.createMetricsSystem(conf).registerSource(taskRunnerSource);
  }

  @Override public void run() {
    List<Object> params = tw.paramValues;
    Object[] args = null;
    if (params != null && !params.isEmpty()) {
      args = params.toArray(new Object[] {});
    }
    Timer.Context context = null;
    try {
      String threadName = Thread.currentThread().getName();
      logger.info("Begin to run task {}, threadName: {}", tw.task, threadName);
//      context = taskRunnerSource.taskExecuteTimer.time();
      method.invoke(instance, args);
      worker.addToReportQueue(new TaskResult(new FinishTask(tw.task, "success"), tw.timerContext));
      logger.info("Finish run task {}, threadName: {}", tw.task, threadName);
    } catch (Throwable e) {
      worker.addToReportQueue(new TaskResult(new FinishTask(tw.task, Throwables.getStackTraceAsString(e)), tw.timerContext));
      logger.error("Invoke method {} of task {} failed", method, tw.task, e);
    } finally {
      if (context != null) {
        context.stop();
      }
    }
  }
}
