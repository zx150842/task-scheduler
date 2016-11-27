package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;
import com.dts.rpc.network.buffer.NettyManagedBuffer;

import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public final class RpcRequest extends AbstractMessage implements RequestMessage {

  public final long requestId;

  public RpcRequest(long requestId, ManagedBuffer message) {
    super(message);
    this.requestId = requestId;
  }

  @Override
  public Type type() {
    return null;
  }

  @Override
  public int encodeLength() {
    return 0;
  }

  @Override
  public void encode(ByteBuf buf) {

  }

  public static RpcRequest decode(ByteBuf buf) {
    long requestId = buf.readLong();
    buf.readInt();
    return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
  }
}
