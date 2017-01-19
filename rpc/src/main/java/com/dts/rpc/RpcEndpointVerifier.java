package com.dts.rpc;

import com.dts.rpc.exception.DTSException;
import com.dts.rpc.netty.Dispatcher;
import com.dts.rpc.netty.NettyRpcEnv;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class RpcEndpointVerifier extends RpcEndpoint {
  public static final String NAME = "endpoint-verifier";
  private final Dispatcher dispatcher;

  public RpcEndpointVerifier(NettyRpcEnv rpcEnv, Dispatcher dispatcher) {
    super(rpcEnv);
    this.dispatcher = dispatcher;
  }

  @Override public void receive(Object o) {
    throw new DTSException("RpcEndpointVerfier does not implement 'receive'");
  }

  @Override public void receiveAndReply(Object o, RpcCallContext context) {
    if (o instanceof CheckExistence) {
      CheckExistence msg = (CheckExistence)o;
      context.reply(dispatcher.verify(msg.name));
    }
  }

  public static class CheckExistence implements Serializable {
    public final String name;

    public CheckExistence(String name) {
      this.name = name;
    }
  }
}
