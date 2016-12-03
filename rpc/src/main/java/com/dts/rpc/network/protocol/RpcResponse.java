package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;
import com.dts.rpc.network.buffer.NettyManagedBuffer;

import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public class RpcResponse extends AbstractMessage implements ResponseMessage {
  public final long requestId;

  public RpcResponse(long requestId, ManagedBuffer message) {
    super(message, true);
    this.requestId = requestId;
  }

  @Override
  public Type type() {
    return Type.RpcResponse;
  }

  @Override
  public int encodeLength() {
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeInt((int) body().size());
  }

  public static RpcResponse decode(ByteBuf buf) {
    long requestId = buf.readLong();
    buf.readInt();
    return new RpcResponse(requestId, new NettyManagedBuffer(buf.retain()));
  }
}
