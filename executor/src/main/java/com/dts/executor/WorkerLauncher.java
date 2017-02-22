package com.dts.executor;

import com.dts.core.rpc.RpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.DTSConf;
import com.dts.executor.task.TaskMethodWrapper;
import com.dts.executor.task.TaskScanner;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 执行器的启动入口, 应用项目需要注册这个类，并可以通过继承这个类重写{@link #getConfig()}方法。
 * 这里执行器应用的项目需要为spring项目，在执行器所属项目启动时， 启动器会被调用，
 * 进行初始化工作并在当前节点启动执行器
 *
 * @author zhangxin
 */
public abstract class WorkerLauncher implements InitializingBean, DisposableBean {

  @Autowired
  private TaskScanner taskScanner;
  private RpcEnv rpcEnv;

  public final void start() {
    DTSConf conf = getConfig();
    int port = conf.getInt("dts.worker.port", 0);
    String packageName = conf.get("dts.worker.packageName");
    TaskMethodWrapper tw = taskScanner.getTaskMethodWrapper(packageName);
    Worker worker = Worker.launchWorker(AddressUtil.getLocalHost(), port, tw, conf);
    rpcEnv = worker.rpcEnv();
  }

  /**
   * 可以被子类覆盖，重写读取配置文件的方法
   *
   * @return
   */
  protected DTSConf getConfig() {
    return DTSConfUtil.readFile("dts.properties");
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  @Override
  public void destroy() throws Exception {
    rpcEnv.shutdown();
    rpcEnv.awaitTermination();
  }
}
