package com.dts.admin;

import com.dts.core.DTSConf;
import com.dts.core.util.DTSConfUtil;

import org.junit.Test;

/**
 * @author zhangxin
 */
public class DTSConfTest {

  @Test
  public void test() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    System.out.println(conf.get("dts.master.zookeeper.url"));
  }
}
