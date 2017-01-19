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

  public void receive(Object o) {
    throw new RuntimeException(self() + " does not implement 'receive'");
  }

  public void receiveAndReply(Object o, RpcCallContext context) throws Exception {
    throw new RuntimeException(self() + " does not implement 'receiveAndReply'");
  }

  public void onError(Throwable cause) throws Throwable { throw cause; }

  public void onConnected(RpcAddress remoteAddress) {}

  public void onDisconnected(RpcAddress remoteAddress) {}

  public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {}

  public void onStart() {}

  public void onStop() {}

  public final void stop() {
    RpcEndpointRef self = self();
    if (self != null) {
      rpcEnv.stop(self);
    }
  }

  protected RpcEndpointRef self() {
    Preconditions.checkNotNull(rpcEnv, "rpcEnv has not been initialized");
    return rpcEnv.endpointRef(this);
  }
}
