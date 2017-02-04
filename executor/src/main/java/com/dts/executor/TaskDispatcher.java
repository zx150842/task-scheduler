package com.dts.executor;

import com.google.common.collect.Queues;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.util.ThreadUtil;
import com.dts.core.DTSConf;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.executor.task.TaskMethodWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zhangxin
 */
public class TaskDispatcher {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final LinkedBlockingQueue<TaskWrapper> receivers = Queues.newLinkedBlockingQueue();
  private final ThreadPoolExecutor threadpool;
  private final ExecutorService dispatchThread;
  private final TaskMethodWrapper taskMethodWrapper;
  private final Worker worker;

  public TaskDispatcher(DTSConf conf, Worker worker, TaskMethodWrapper taskMethodWrapper) {
    this.worker = worker;
    this.taskMethodWrapper = taskMethodWrapper;
    int threadNum = conf.getInt("dts.worker.threadPool.threadCount", 10);
    threadpool = ThreadUtil.newDaemonFixedThreadPool(threadNum, "worker-task-threadpool");
    dispatchThread = ThreadUtil.newDaemonSingleThreadExecutor("worker-dispatcher");
    dispatchThread.submit(new TaskLoop());
  }

  public boolean addTask(TaskWrapper tw) {
    try {
      return receivers.add(tw);
    } catch (Throwable e) {
      logger.error("Couldn't add task {} to task queue", e);
      throw new RuntimeException(e);
    }
  }

  private class TaskLoop implements Runnable {

    @Override public void run() {
      try {
        while (true) {
          TaskWrapper tw = receivers.take();
          String taskName = tw.task.getTaskName();
          Method method = taskMethodWrapper.taskMethods.get(taskName);
          if (method == null) {
            logger.error("Cannot find task method for task name {}, ignore task {}", taskName, tw.task);
          } else {
            Object instance = taskMethodWrapper.taskBeans.get(taskName);
            TaskRunner taskRunner = new TaskRunner(worker, tw, method, instance);
            threadpool.submit(taskRunner);
          }
        }
      } catch (InterruptedException e) {
        // exit
      } catch (Exception e) {
        logger.error("Submit task {} to thread pool error.", e);
      }
    }
  }
}
