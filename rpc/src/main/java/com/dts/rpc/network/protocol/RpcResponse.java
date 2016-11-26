package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;

/**
 * @author zhangxin
 */
public class RpcResponse extends AbstractMessage {
  public final long requestId;

  public RpcResponse(long requestId, ManagedBuffer message) {
    super(message);
    this.requestId = requestId;
  }
}
