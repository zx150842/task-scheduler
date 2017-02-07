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
 * 执行器的启动入口。这里执行器应用的项目需要为spring项目，在执行器所属项目启动时，
 * 启动器会被调用，进行初始化工作并在当前节点启动执行器
 *
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
