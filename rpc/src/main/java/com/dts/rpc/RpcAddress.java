package com.dts.rpc;

/**
 * @author zhangxin
 */
public class RpcAddress {

  private final String host;
  private final int port;

  public RpcAddress(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
}
