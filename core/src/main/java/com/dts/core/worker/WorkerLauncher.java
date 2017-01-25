package com.dts.core.worker;

import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.rpc.DTSConf;
import com.dts.rpc.exception.DTSException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author zhangxin
 */
public class WorkerLauncher implements InitializingBean {

  public static void start() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    int port = conf.getInt("dts.worker.port", 0);
    String[] masterUrls = getMasterUrls(conf);
    Worker.launchWorker(AddressUtil.getLocalHost(), port, masterUrls, conf);
  }

  private static String[] getMasterUrls(DTSConf conf) {
    String masterUrlStr = conf.get("dts.master.url");
    String[] masterUrls = StringUtils.split(masterUrlStr, ",");
    if (masterUrls != null && masterUrls.length > 0) {
      return masterUrls;
    }
    throw new DTSException("Please config 'dts.master.url'");
  }

  @Override public void afterPropertiesSet() throws Exception {
    start();
  }
}
