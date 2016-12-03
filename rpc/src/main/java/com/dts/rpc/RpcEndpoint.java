package com.dts.rpc;

/**
 * @author zhangxin
 */
public abstract class RpcEndpoint {

  public abstract void receive(Object o);

  public abstract void receiveAndReply(RpcCallContext context);

  public void onError(Throwable cause) throws Throwable { throw cause; }

  public void onConnected(RpcAddress remoteAddress) {}

  public void onDisconnected(RpcAddress remoteAddress) {}

  public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {}

  public void onStart() {}

  public void onStop() {}
}
