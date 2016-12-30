package com.dts.core.worker.ioc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @author zhangxin
 */
public abstract class IOC {
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public abstract <T> T get(Class<T> clazz, Method method);
}
