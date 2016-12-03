package com.dts.rpc;

/**
 * @author zhangxin
 */
public class RpcEndpointAddress {

  private final RpcAddress rpcAddress;
  private final String name;

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
}