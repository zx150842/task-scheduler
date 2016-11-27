package com.dts.rpc.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public class RpcFailure extends AbstractMessage {
  public final long requestId;
  public final String errorString;

  public RpcFailure(long requestId, String errorString) {
    this.requestId = requestId;
    this.errorString = errorString;
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

  public static RpcFailure decode(ByteBuf buf) {
    long requestId = buf.readLong();
    String errorString = Encoders.Strings.decode(buf);
    return new RpcFailure(requestId, errorString);
  }
}
