package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;
import com.dts.rpc.network.buffer.NettyManagedBuffer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public class OneWayMessage extends AbstractMessage implements RequestMessage {

  public OneWayMessage(ManagedBuffer body) {
    super(body, true);
  }

  @Override public int encodeLength() {
    return 4;
  }

  @Override public void encode(ByteBuf buf) {
    buf.writeInt((int) body().size());
  }

  @Override public Type type() {
    return Type.OneWayMessage;
  }

  public static OneWayMessage decode(ByteBuf buf) {
    buf.readInt();
    return new OneWayMessage(new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OneWayMessage) {
      OneWayMessage o = (OneWayMessage)other;
      return super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("body", body())
      .toString();
  }
}
