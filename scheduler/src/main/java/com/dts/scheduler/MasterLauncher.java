package com.dts.scheduler;

import com.dts.core.DTSConf;
import com.dts.core.util.DTSConfUtil;

/**
 * @author zhangxin
 */
public class MasterLauncher {

  public static void start() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    Master master = Master.launchMaster(conf);
    master.rpcEnv().awaitTermination();
  }
}
