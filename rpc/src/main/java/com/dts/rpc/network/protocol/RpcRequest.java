package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;

/**
 * @author zhangxin
 */
public final class RpcRequest extends AbstractMessage implements RequestMessage {

  public final long requestId;

  public RpcRequest(long requestId, ManagedBuffer message) {
    super(message);
    this.requestId = requestId;
  }
}
