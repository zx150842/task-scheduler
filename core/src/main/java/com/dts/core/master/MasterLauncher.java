package com.dts.core.master;

import com.dts.core.util.DTSConfUtil;
import com.dts.rpc.DTSConf;

/**
 * @author zhangxin
 */
public class MasterLauncher {

  public void start() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    Master.launchMaster(conf);
  }
}
