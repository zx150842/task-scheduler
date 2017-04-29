package com.dts.executor;

import com.dts.core.rpc.RpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.DTSConf;
import com.dts.executor.task.TaskMethodWrapper;
import com.dts.executor.task.TaskScanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Iterator;

/**
 * 执行器的启动入口, 当前执行器由于使用了{@link PropertySource}配置方法，
 * 外部应用项目需要使用spring boot
 *
 * @author zhangxin
 */
@Component
@org.springframework.context.annotation.PropertySource(
  "classpath:application-${spring.profiles.active}.properties")
public class WorkerLauncher implements InitializingBean, DisposableBean {

  @Autowired
  private TaskScanner taskScanner;
  @Autowired
  private Environment env;
  private RpcEnv rpcEnv;
  private final Logger logger = LoggerFactory.getLogger(WorkerLauncher.class);

  public final void start() {
    DTSConf conf = getConfig();
    int port = conf.getInt("dts.worker.port", 0);
    String packageName = conf.get("dts.worker.packageName");
    TaskMethodWrapper tw = taskScanner.getTaskMethodWrapper(packageName);
    if (tw == null
        || tw.taskMethodDetails == null
        || tw.taskBeans == null
        || tw.taskMethods == null) {
      logger.error("There is no task found in current app, not to start worker and just return");
      return;
    }
    Worker worker = Worker.launchWorker(AddressUtil.getLocalHost(), port, tw, conf);
    rpcEnv = worker.rpcEnv();
  }

  /**
   * 可以被子类覆盖，重写读取配置文件的方法
   *
   * @return
   */
  protected DTSConf getConfig() {
    DTSConf conf = new DTSConf(false);
    for (Iterator it = ((AbstractEnvironment)env).getPropertySources().iterator(); it.hasNext();) {
      PropertySource propertySource = (PropertySource) it.next();
      if (propertySource instanceof MapPropertySource) {
        MapPropertySource mapPropertySource = (MapPropertySource) propertySource;
        for (String key : mapPropertySource.getSource().keySet()) {
          if (key.startsWith("dts")) {
            conf.set(key, (String) mapPropertySource.getSource().get(key));
          }
        }
      }
    }
    return conf;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  @Override
  public void destroy() throws Exception {
    if (rpcEnv != null) {
      rpcEnv.shutdown();
      rpcEnv.awaitTermination();
    }
  }
}
