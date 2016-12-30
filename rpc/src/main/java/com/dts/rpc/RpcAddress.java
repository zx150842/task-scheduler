package com.dts.rpc;

/**
 * @author zhangxin
 */
public class RpcAddress {

  private final String host;
  private final int port;
  private final String hostPort;

  public RpcAddress(String host, int port) {
    this.host = host;
    this.port = port;
    this.hostPort = host + ":" + port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getHostPort() {
    return hostPort;
  }

  @Override
  public String toString() {
    return hostPort;
  }
}
