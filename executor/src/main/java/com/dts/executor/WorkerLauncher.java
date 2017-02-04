package com.dts.executor;

import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.DTSConf;
import com.dts.executor.task.TaskMethodWrapper;
import com.dts.executor.task.TaskScanner;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author zhangxin
 */
@Component
public class WorkerLauncher implements InitializingBean {

  @Autowired
  private TaskScanner taskScanner;

  public void start() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    int port = conf.getInt("dts.worker.port", 0);
    String packageName = conf.get("dts.worker.packageName");
    TaskMethodWrapper tw = taskScanner.getTaskMethodWrapper(packageName);
    Worker.launchWorker(AddressUtil.getLocalHost(), port, tw, conf);
  }

  @Override public void afterPropertiesSet() throws Exception {
    start();
  }
}
