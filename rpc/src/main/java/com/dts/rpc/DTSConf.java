package com.dts.rpc;

import com.dts.rpc.network.util.ConfigProvider;
import com.dts.rpc.network.util.TransportConf;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author zhangxin
 */
public class DTSConf implements Cloneable {
  private final Map<String, String> settings = Maps.newConcurrentMap();

  public DTSConf(boolean loadDefaults) {
    if (loadDefaults) {
      loadFromSystemProperties();
    }
  }

  private void loadFromSystemProperties() {
    Set<String> propertyNames = System.getProperties().stringPropertyNames();
    for (String propertyName : propertyNames) {
      settings.put(propertyName, System.getProperty(propertyName));
    }
  }

  public String get(String key) {
    if (settings.containsKey(key)) { settings.get(key); }
    throw new NoSuchElementException(key);
  }

  public String get(String key, String defaultValue) {
    return settings.containsKey(key) ? settings.get(key) : defaultValue;
  }

  public int getInt(String key, int defaultValue) {
    return NumberUtils.toInt(settings.get(key), defaultValue);
  }

  public long getLong(String key, long defaultValue) {
    return NumberUtils.toLong(settings.get(key), defaultValue);
  }

  public double getDouble(String key, double defaultValue) {
    return NumberUtils.toDouble(settings.get(key), defaultValue);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    return settings.containsKey(key) ? Boolean.valueOf(settings.get(key)) : defaultValue;
  }

  public TransportConf getTransportConf(String module) {
    DTSConf conf = this.clone();
    return new TransportConf(module, new ConfigProvider() {
      @Override public String get(String name) {
        return conf.get(name);
      }
    });
  }

  private void set(String key, String value) {
    settings.put(key, value);
  }

  @Override
  public DTSConf clone() {
    DTSConf cloned = new DTSConf(false);
    for (String key : settings.keySet()) {
      cloned.set(key, settings.get(key));
    }
    return cloned;
  }
}
