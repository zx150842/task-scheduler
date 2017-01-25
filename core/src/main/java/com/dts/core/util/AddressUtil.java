package com.dts.core.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zhangxin
 */
public class AddressUtil {
  public static String getLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
