package com.dts.core.util;

import com.dts.core.exception.DTSException;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * @author zhangxin
 */
public class AddressUtil {
  public static String getLocalHost() {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        NetworkInterface ni = interfaces.nextElement();
        Enumeration<InetAddress> nias = ni.getInetAddresses();
        while (nias.hasMoreElements()) {
          InetAddress ia = nias.nextElement();
          if (!ia.isLinkLocalAddress()
            && !ia.isLoopbackAddress()
            && ia instanceof Inet4Address) {
            return ia.getHostAddress();
          }
        }
      }
      throw new DTSException("Cannot find local host ip");
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }
}
