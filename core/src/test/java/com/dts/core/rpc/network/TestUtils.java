package com.dts.core.rpc.network;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author zhangxin
 */
public class TestUtils {
  public static String getLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
