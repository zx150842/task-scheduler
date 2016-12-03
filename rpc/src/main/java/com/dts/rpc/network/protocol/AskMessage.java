package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;
import com.dts.rpc.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public class AskMessage extends AbstractMessage implements RequestMessage {

  public AskMessage(ManagedBuffer body) {
    super(body, true);
  }

  @Override public int encodeLength() {
    return 4;
  }

  @Override public void encode(ByteBuf buf) {
    buf.writeInt((int) body().size());
  }

  @Override public Type type() {
    return Type.AskMessage;
  }

  public static AskMessage decode(ByteBuf buf) {
    buf.readInt();
    return new AskMessage(new NettyManagedBuffer(buf.retain()));
  }
}
