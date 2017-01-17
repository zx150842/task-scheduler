package com.dts.core.util;

import com.dts.rpc.DTSConf;

import java.io.IOException;
import java.util.Properties;

/**
 * @author zhangxin
 */
public class DTSConfUtil {

  public static DTSConf readFile(String path) {
    DTSConf conf = new DTSConf(true);
    Properties prop = new Properties();
    try {
      prop.load(DTSConfUtil.class.getResourceAsStream(path));
      for (Object key : prop.keySet()) {
        conf.set((String)key, (String)prop.get(key));
      }
      return conf;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
