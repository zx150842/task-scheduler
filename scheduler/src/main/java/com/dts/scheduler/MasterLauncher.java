package com.dts.scheduler;

import com.dts.core.DTSConf;
import com.dts.core.EndpointNames;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.scheduler.queue.TaskQueueContext;

/**
 * 启动master的主程序入口
 *
 * @author zhangxin
 */
public class MasterLauncher {
  private static RpcEnv rpcEnv;

  public static void main(String[] args) {
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    String propertyFilePath = "dts.properties";
    DTSConf conf = DTSConfUtil.readFile(propertyFilePath);
    Master master = Master.launchMaster(conf);
    rpcEnv = master.rpcEnv();
    rpcEnv.awaitTermination();
  }

  static class ShutdownHook extends Thread {
    @Override
    public void run() {
      System.out.println("shutdown");
      if (rpcEnv != null) {
        rpcEnv.shutdown();
      }
    }
  }
}
