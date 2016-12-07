package com.dts.rpc;

/**
 * @author zhangxin
 */
public class RpcEndpointVerifier extends RpcEndpoint {
  @Override public void receive(Object o) {
    throw new RuntimeException("RpcEndpointVerfier does not implement 'receive'");
  }

  @Override public void receiveAndReply(RpcCallContext context) {

  }
}
