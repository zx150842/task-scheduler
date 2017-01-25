package com.dts.core.worker;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.exception.DTSConfException;
import com.dts.core.util.AnnotationUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.core.worker.ioc.IOC;
import com.dts.core.worker.ioc.SpringIOC;
import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcEndpointRef;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zhangxin
 */
public class TaskDispatcher {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final LinkedBlockingQueue<TaskWrapper> receivers = Queues.newLinkedBlockingQueue();
  private final TaskThreadPoolExecutor threadpool;
  private final ExecutorService dispatchThread;
  private final DTSConf conf;
  private final Map<String, Method> taskMethodMap = Maps.newHashMap();
  private final IOC ioc;
  private final RpcEndpointRef endpoint;

  public TaskDispatcher(DTSConf conf, RpcEndpointRef endpoint) {
    this.conf = conf;
    int threadNum = conf.getInt("dts.worker.threadPool.threadCount", 10);
    threadpool = ThreadUtil.newDaemonTaskThreadPool(threadNum, "worker-threadpool", endpoint);
    dispatchThread = ThreadUtil.newDaemonSingleThreadExecutor("worker-dispatcher");
    dispatchThread.submit(new TaskLoop());
    this.endpoint = endpoint;

    String iocType = conf.get("dts.worker.iocType", "spring");
    if ("spring".equals(iocType)) {
      ioc = new SpringIOC();
    } else {
      throw new DTSConfException("Invalid conf of 'dts.worker.iocType': " + StringUtils.trimToEmpty(iocType));
    }
  }

  public void onStart() {
    String packageName = conf.get("dts.worker.basePackage");
    List<Method> methods = AnnotationUtil.getMethods(packageName, Task.class);
    for (Method method : methods) {
      Task taskAnnotation = method.getAnnotation(Task.class);
      taskMethodMap.put(taskAnnotation.name(), method);
    }
  }

  public boolean stopTask(String id) {
    return threadpool.stopTask(id);
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
          threadpool.submit(new Runnable() {
            @Override public void run() {
              Method method = taskMethodMap.get(tw.task.getTaskName());
              if (method == null) {
                logger.error("Cannot find task method for task name {}, ignore task {}", tw.task.getTaskName(), tw.task);
              } else {
                List<Object> params = tw.paramValues;
                Object[] args = null;
                if (params != null && !params.isEmpty()) {
                  args = params.toArray(new Object[]{});
                }
                try {
                  Object instance = ioc.get(method.getClass(), method);
                  method.invoke(instance, args);
                } catch (Throwable e) {
                  logger.error("Invoke method {} of task {} failed", method, tw.task, e);
                }
              }
            }
          });
        }
      } catch (InterruptedException e) {
        // exit
      } catch (Exception e) {
        logger.error("Submit task {} to thread pool error.", e);
      }
    }
  }
}
