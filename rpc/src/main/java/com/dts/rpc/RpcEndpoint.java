package com.dts.rpc;

import com.google.common.base.Preconditions;

/**
 * @author zhangxin
 */
public abstract class RpcEndpoint {

  protected RpcEnv rpcEnv;

  public RpcEndpoint(RpcEnv rpcEnv) {
    this.rpcEnv = rpcEnv;
  }

  public abstract void receive(Object o);

  public abstract void receiveAndReply(Object o, RpcCallContext context);

  public void onError(Throwable cause) throws Throwable { throw cause; }

  public void onConnected(RpcAddress remoteAddress) {}

  public void onDisconnected(RpcAddress remoteAddress) {}

  public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {}

  public void onStart() {}

  public void onStop() {}

  protected RpcEndpointRef self() {
    Preconditions.checkNotNull(rpcEnv, "rpcEnv has not been initialized");
    return rpcEnv.endpointRef(this);
  }
}
