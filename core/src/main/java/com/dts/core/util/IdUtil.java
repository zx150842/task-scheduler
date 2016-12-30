package com.dts.core.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhangxin
 */
public class IdUtil {

  private final AtomicLong count = new AtomicLong(System.currentTimeMillis());

  private static final IdUtil instance = new IdUtil();

  private IdUtil() {}

  public static final String getUniqId() {
    return String.valueOf(instance.count.getAndIncrement());
  }
}
