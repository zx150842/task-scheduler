package com.dts.core.worker;

import com.dts.core.TaskConf;
import com.dts.core.TaskInfo;
import com.dts.core.exception.DTSConfException;
import com.dts.core.util.AnnotationUtil;
import com.dts.core.util.ThreadUtils;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zhangxin
 */
public class TaskDispatcher {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final LinkedBlockingQueue<TaskInfo> receivers = Queues.newLinkedBlockingQueue();
  private final TaskThreadPoolExecutor threadpool;
  private final ExecutorService dispatchThread;
  private final DTSConf conf;
  private final Map<String, Method> taskMethodMap = Maps.newHashMap();
  private final IOC ioc;
  private final RpcEndpointRef endpoint;

  public TaskDispatcher(DTSConf conf, RpcEndpointRef endpoint) {
    this.conf = conf;
    int threadNum = conf.getInt("dts.worker.threadPool.threadCount", 10);
    threadpool = ThreadUtils.newDaemonTaskThreadPool(threadNum, "worker-threadpool", endpoint);
    dispatchThread = ThreadUtils.newDaemonSingleThreadExecutor("worker-dispatcher");
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

  public boolean addTask(TaskInfo task) {
    try {
      return receivers.add(task);
    } catch (Throwable e) {
      logger.error("Couldn't add task {} to task queue", e);
      throw new RuntimeException(e);
    }
  }

  private class TaskLoop implements Runnable {

    @Override public void run() {
      try {
        while (true) {
          TaskInfo task = receivers.take();
          threadpool.submit(new Runnable() {
            @Override public void run() {
              Method method = taskMethodMap.get(task.taskConf.getTaskId());
              if (method == null) {
                logger.error("Cannot find task method for task id {}, ignore task {}", task.taskConf.getTaskId(), task.taskConf);
              } else {
                LinkedHashMap<String, String> params = task.taskConf.getParams();
                String[] args = null;
                if (params != null && !params.isEmpty()) {
                  args = params.values().toArray(new String[]{});
                }
                try {
                  Object instance = ioc.get(method.getClass(), method);
                  method.invoke(instance, args);
                } catch (Throwable e) {
                  logger.error("Invoke method {} of task {} failed", method, task.taskConf, e);
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
