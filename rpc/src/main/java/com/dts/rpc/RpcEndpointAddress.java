package com.dts.rpc;

/**
 * @author zhangxin
 */
public class RpcEndpointAddress {

  private final RpcAddress rpcAddress;
  private final String name;

  public RpcEndpointAddress(String host, int port, String name) {
    this(new RpcAddress(host, port), name);
  }

  public RpcEndpointAddress(RpcAddress rpcAddress, String name) {
    this.rpcAddress = rpcAddress;
    this.name = name;
  }

  public RpcAddress getRpcAddress() {
    return rpcAddress;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    if (rpcAddress != null) {
      return String.format("%s@%s:%s", name, rpcAddress.host, rpcAddress.port);
    } else {
      return name;
    }
  }
}
