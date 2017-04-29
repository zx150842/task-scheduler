package com.dts.executor;

import com.dts.core.metrics.MetricsSystem;
import com.google.common.collect.Queues;

import com.dts.core.util.ThreadUtil;
import com.dts.core.DTSConf;
import com.dts.executor.task.TaskMethodWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 任务分发器，不断的取出执行器的任务队列中的任务，并从线程池中选择一个线程执行
 *
 * @author zhangxin
 */
public class TaskDispatcher {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final LinkedBlockingQueue<TaskWrapper> receivers = Queues.newLinkedBlockingQueue();
  private final ThreadPoolExecutor threadPool;
  private final ExecutorService dispatchThread;
  private final TaskMethodWrapper taskMethodWrapper;
  private final DTSConf conf;
  private final Worker worker;
  private final TaskDispatcherSource taskDispatcherSource;

  public TaskDispatcher(DTSConf conf, Worker worker, TaskMethodWrapper taskMethodWrapper) {
    this.conf = conf;
    this.worker = worker;
    this.taskMethodWrapper = taskMethodWrapper;
    this.dispatchThread = ThreadUtil.newDaemonSingleThreadExecutor("worker-dispatcher");
    this.dispatchThread.submit(new TaskLoop());
    int threadNum = worker.THREAD_NUM;
    this.threadPool = ThreadUtil.newDaemonFixedThreadPool(threadNum, "worker-task-threadpool");
    this.taskDispatcherSource = new TaskDispatcherSource(this);
    MetricsSystem.createMetricsSystem(conf).registerSource(taskDispatcherSource);
  }

  public boolean addTask(TaskWrapper tw) {
    try {
      return receivers.add(tw);
    } catch (Throwable e) {
      logger.error("Couldn't add task {} to task queue", e);
      throw new RuntimeException(e);
    }
  }

  public ThreadPoolExecutor threadpool() {
    return threadPool;
  }

  private class TaskLoop implements Runnable {

    @Override public void run() {
        while (true) {
          try {
            TaskWrapper tw = receivers.take();
            String taskName = tw.task.getTaskName();
            Method method = taskMethodWrapper.taskMethods.get(taskName);
            if (method == null) {
              logger.error("Cannot find task method for task name {}, ignore task {}", taskName, tw.task);
            } else {
              Object instance = taskMethodWrapper.taskBeans.get(taskName);
              TaskRunner taskRunner = new TaskRunner(worker, conf, tw, method, instance);
              threadPool.submit(taskRunner);
            }
          } catch (InterruptedException e) {
            // ignore
          } catch (Exception e) {
            logger.error("Submit task {} to thread pool error.", e);
          }
        }
    }
  }
}
