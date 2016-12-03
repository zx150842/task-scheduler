package com.dts.rpc;

import com.dts.rpc.RpcAddress;

/**
 * @author zhangxin
 */
public interface RpcCallContext {

  void reply(Object response);

  void sendFailure(Throwable cause);

  RpcAddress senderAddress();
}


