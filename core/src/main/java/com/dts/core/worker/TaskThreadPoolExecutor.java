package com.dts.core.worker;

import com.dts.core.DeployMessages;
import com.dts.core.TaskInfo;
import com.dts.rpc.RpcEndpointRef;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangxin
 */
public class TaskThreadPoolExecutor extends ThreadPoolExecutor {

  private final Map<String, TaskRunner> taskRunners = Maps.newConcurrentMap();
  private final Map<String, Thread> taskRunThreads = Maps.newConcurrentMap();
  private final RpcEndpointRef endpoint;

  public TaskThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
    TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RpcEndpointRef endpoint) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    this.endpoint = endpoint;
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    TaskRunner runner = (TaskRunner)r;
    try {
      runner.lock.lock();
      taskRunThreads.put(runner.task.getId(), t);
      endpoint.send(new DeployMessages.ExecutingTask(runner.task, t.getName()));
    } finally {
      runner.lock.unlock();
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    TaskRunner runner = (TaskRunner)r;
    try {
      runner.lock.lock();
      taskRunThreads.remove(runner.task.getId());
      if (t == null) {
        endpoint.send(new DeployMessages.FinishTask(runner.task, "success"));
      } else {
        endpoint.send(new DeployMessages.FinishTask(runner.task, t.toString()));
      }
    } finally {
      runner.lock.unlock();
    }
  }

  public boolean stopTask(String id) {
    TaskRunner runner = taskRunners.get(id);
    remove(runner);
    Thread t = taskRunThreads.get(id);
    if (t == null) {
      return false;
    }
    boolean interrupted = false;
    try {
      runner.lock.lock();
      if (taskRunThreads.containsKey(id)) {
        t.interrupt();
        interrupted = true;
      }
    } finally {
      runner.lock.unlock();
    }
    return interrupted;
  }

  //TODO stop thread if cannot interrupt thread

  public static class TaskRunner implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public final TaskInfo task;
    public final Method method;
    public final Object instance;
    public final ReentrantLock lock = new ReentrantLock();

    public TaskRunner(TaskInfo task, Method method, Object instance) {
      this.task = task;
      this.method = method;
      this.instance = instance;
    }

    @Override public void run() {
      LinkedHashMap<String, String> params = task.taskConf.getParams();
      String[] args = null;
      if (params != null && !params.isEmpty()) {
        args = params.values().toArray(new String[] {});
      }
      try {
        method.invoke(instance, args);
      } catch (Throwable e) {
        logger.error("Invoke method {} of task {} failed", method, task, e);
      }
    }
  }
}
