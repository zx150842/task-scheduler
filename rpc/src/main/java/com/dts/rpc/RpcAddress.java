package com.dts.rpc;

/**
 * @author zhangxin
 */
public class RpcAddress {

  public final String host;
  public final int port;
  public final String hostPort;

  public RpcAddress(String host, int port) {
    this.host = host;
    this.port = port;
    this.hostPort = host + ":" + port;
  }

  @Override
  public String toString() {
    return hostPort;
  }
}
