package com.dts.core.rpc.network.util;

import java.util.NoSuchElementException;

/**
 * @author zhangxin
 */
public class SystemPropertyConfigProvider extends ConfigProvider {

  @Override public String get(String name) {
    String value = System.getProperty(name);
    if (value == null) {
      throw new NoSuchElementException(name);
    }
    return value;
  }
}
