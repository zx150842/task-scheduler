package com.dts.core.rpc.network.util;

import java.util.NoSuchElementException;

/**
 * Created by zhangxin on 2016/11/27.
 */
public abstract class ConfigProvider {

  public abstract String get(String name);

  public String get(String name, String defaultValue) {
    try {
      return get(name);
    } catch (NoSuchElementException e) {
      return defaultValue;
    }
  }

  public int getInt(String name, int defaultValue) {
    return Integer.parseInt(get(name, Integer.toString(defaultValue)));
  }

  public long getLong(String name, int defaultValue) {
    return Long.parseLong(get(name, Long.toString(defaultValue)));
  }

  public double getDouble(String name, double defaultValue) {
    return Double.parseDouble(get(name, Double.toString(defaultValue)));
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    return Boolean.parseBoolean(get(name, Boolean.toString(defaultValue)));
  }
}
