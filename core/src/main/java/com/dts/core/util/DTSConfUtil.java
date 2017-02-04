package com.dts.core.util;

import com.dts.core.DTSConf;
import com.dts.core.exception.DTSException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author zhangxin
 */
public class DTSConfUtil {

  public static DTSConf readFile(String path) {
    DTSConf conf = new DTSConf(true);
    Properties prop = new Properties();
    try {
      InputStream in = DTSConfUtil.class.getClassLoader().getResourceAsStream(path);
      prop.load(in);
      for (Object key : prop.keySet()) {
        conf.set((String)key, (String)prop.get(key));
      }
      return conf;
    } catch (IOException e) {
      throw new DTSException(e);
    }
  }
}
