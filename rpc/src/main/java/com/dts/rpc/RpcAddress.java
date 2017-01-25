package com.dts.rpc;

import com.google.common.base.Objects;
import com.sun.deploy.util.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class RpcAddress implements Serializable {

  public final String host;
  public final int port;
  public final String hostPort;

  public RpcAddress(String host, int port) {
    this.host = host;
    this.port = port;
    this.hostPort = host + ":" + port;
  }

  public static RpcAddress fromURL(String url) {
    String[] hostPort = StringUtils.splitString(url, ":");
    if (hostPort == null || hostPort.length != 2) {
      return null;
    }
    String host = hostPort[0];
    int port = NumberUtils.toInt(hostPort[1]);
    return new RpcAddress(host, port);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hostPort);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcAddress) {
      RpcAddress o = (RpcAddress)other;
      return hostPort.equals(o.hostPort);
    }
    return false;
  }

  @Override
  public String toString() {
    return hostPort;
  }
}
