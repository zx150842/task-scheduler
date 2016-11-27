package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;

import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public interface Message extends Encodable {

  Type type();

  ManagedBuffer body();

  boolean isBodyInFrame();

  enum Type implements Encodable {
    RpcRequest(0), RpcResponse(1), RpcFailure(2), User(-1);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() {
      return id;
    }

    @Override
    public int encodeLength() {
      return 1;
    }

    @Override
    public void encode(ByteBuf buf) {
      buf.writeByte(id);
    }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0: return RpcRequest;
        case 1: return RpcResponse;
        case 2: return RpcFailure;
        case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
        default: throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }
}
