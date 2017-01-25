package com.dts.core.util;

import com.dts.core.worker.TaskThreadPoolExecutor;
import com.dts.rpc.RpcEndpointRef;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @author zhangxin
 */
public class ThreadUtil {

  public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
    ThreadFactory threadFactory = namedThreadFactory(threadName);
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  public static ThreadPoolExecutor newDaemonCachedThreadPool(String prefix, int maxThreadNumber, int keepAliveTime) {
    ThreadFactory threadFactory = namedThreadFactory(prefix);
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
      maxThreadNumber,
      maxThreadNumber,
      keepAliveTime,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(),
      threadFactory
    );
    threadPool.allowCoreThreadTimeOut(true);
    return threadPool;
  }

  public static ThreadPoolExecutor newDaemonFixedThreadPool(int threadNum, String prefix) {
    ThreadFactory threadFactory = namedThreadFactory(prefix);
    return (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum, threadFactory);
  }

  public static ExecutorService newDaemonSingleThreadExecutor(String threadName) {
    ThreadFactory threadFactory = namedThreadFactory(threadName);
    return Executors.newSingleThreadExecutor(threadFactory);
  }
  
  public static TaskThreadPoolExecutor newDaemonTaskThreadPool(int threadNum, String prefix, RpcEndpointRef endpoint) {
    ThreadFactory threadFactory = namedThreadFactory(prefix);
    return new TaskThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(), threadFactory, endpoint);
  }
  
  public static ThreadFactory namedThreadFactory(String prefix) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build();
  }
}
