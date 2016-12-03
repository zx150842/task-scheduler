package com.dts.rpc.netty;

import com.dts.rpc.RpcAddress;
import com.dts.rpc.RpcCallContext;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.protocol.RpcFailure;

/**
 * @author zhangxin
 */
public abstract class NettyRpcCallContext implements RpcCallContext {

  abstract void send(Object message);

  @Override
  public void reply(Object response) {
    send(response);
  }

  @Override
  public void sendFailure(Throwable cause) {
    send(new RpcFailure(cause));
  }

  @Override
  public RpcAddress senderAddress() {
    return null;
  }
}


class LocalNettyRpcCallContext {
  private RpcAddress senderAddress;

  public LocalNettyRpcCallContext(RpcAddress senderAddress) {
    this.senderAddress = senderAddress;
  }
}

class RemoteNettyRpcCallContext extends NettyRpcCallContext {
  private NettyRpcEnv nettyRpcEnv;
  private RpcResponseCallback callback;
  private RpcAddress senderAddress;

  public RemoteNettyRpcCallContext(
    NettyRpcEnv nettyRpcEnv,
    RpcResponseCallback callback,
    RpcAddress senderAddress) {
    this.nettyRpcEnv = nettyRpcEnv;
    this.callback = callback;
    this.senderAddress = senderAddress;
  }

  @Override void send(Object message) {
    callback.onSuccess(message);
  }
}
