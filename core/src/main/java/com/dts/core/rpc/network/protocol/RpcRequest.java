package com.dts.core.rpc.network.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.dts.core.rpc.network.buffer.ManagedBuffer;
import com.dts.core.rpc.network.buffer.NettyManagedBuffer;

import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public final class RpcRequest extends AbstractMessage implements RequestMessage {

  public final long requestId;

  public RpcRequest(long requestId, ManagedBuffer message) {
    super(message, true);
    this.requestId = requestId;
  }

  @Override
  public Type type() {
    return Type.RpcRequest;
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

  public static RpcRequest decode(ByteBuf buf) {
    long requestId = buf.readLong();
    buf.readInt();
    return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcRequest) {
      RpcRequest o = (RpcRequest)other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("requestId", requestId)
      .add("body", body())
      .toString();
  }
}
