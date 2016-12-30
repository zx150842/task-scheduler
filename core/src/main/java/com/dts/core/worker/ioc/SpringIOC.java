package com.dts.core.worker.ioc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * @author zhangxin
 */
public class SpringIOC extends IOC {
  @Autowired
  private ApplicationContext context;

  public <T> T get(Class<T> clazz, Method method) {
    Map<String, T> map = context.getBeansOfType(clazz);
    if (map == null || map.isEmpty()) {
      logger.error("Cannot find instance of {}", clazz);
    }
    T instance = null;
    for (T bean : map.values()) {
      if (instance == null) {
        instance = bean;
      } else {
        logger.warn("Find multi instances of {}, use first found instance {}", clazz, instance);
      }
    }
    return instance;
  }
}
