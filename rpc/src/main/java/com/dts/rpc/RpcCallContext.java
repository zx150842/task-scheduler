package com.dts.rpc;

/**
 * @author zhangxin
 */
public interface RpcCallContext {

  void reply(Object response);

  void sendFailure(Throwable cause);

  RpcAddress senderAddress();
}


