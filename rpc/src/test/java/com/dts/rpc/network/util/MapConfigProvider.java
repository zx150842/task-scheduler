package com.dts.rpc.network.util;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author zhangxin
 */
public class MapConfigProvider extends ConfigProvider {
  private final Map<String, String> config;

  public MapConfigProvider(Map<String, String> config) {
    this.config = Maps.newHashMap(config);
  }

  @Override
  public String get(String name) {
    String value = config.get(name);
    if (value == null) {
      throw new NoSuchElementException(name);
    }
    return value;
  }
}
