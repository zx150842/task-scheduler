package com.dts.core.rpc.netty;

import com.google.common.util.concurrent.SettableFuture;

import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.RpcCallContext;
import com.dts.core.rpc.network.client.RpcResponseCallback;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
class LocalNettyRpcCallContext implements RpcCallContext {
  private RpcAddress senderAddress;
  private SettableFuture<Object> future;

  public LocalNettyRpcCallContext(RpcAddress senderAddress, SettableFuture<Object> future) {
    this.senderAddress = senderAddress;
    this.future = future;
  }

  @Override
  public void reply(Object response) {
    future.set(response);
  }

  @Override
  public void sendFailure(Throwable cause) {
    future.setException(cause);
  }

  @Override
  public RpcAddress senderAddress() {
    return senderAddress;
  }
}

class RemoteNettyRpcCallContext implements RpcCallContext {
  private NettyRpcEnv nettyRpcEnv;
  private RpcResponseCallback callback;
  private RpcAddress senderAddress;

  public RemoteNettyRpcCallContext(NettyRpcEnv nettyRpcEnv, RpcResponseCallback callback,
                                   RpcAddress senderAddress) {
    this.nettyRpcEnv = nettyRpcEnv;
    this.callback = callback;
    this.senderAddress = senderAddress;
  }

  @Override
  public void reply(Object response) {
    ByteBuffer reply = nettyRpcEnv.serialize(response);
    callback.onSuccess(reply);
  }

  @Override
  public void sendFailure(Throwable cause) {
    ByteBuffer reply = nettyRpcEnv.serialize(cause);
    callback.onSuccess(reply);
  }

  @Override
  public RpcAddress senderAddress() {
    return senderAddress;
  }
}
