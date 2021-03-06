package com.dts.core.rpc.network.client;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public interface RpcResponseCallback {

  void onSuccess(ByteBuffer response);

  void onFailure(Throwable e);
}
