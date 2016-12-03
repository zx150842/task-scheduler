package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public class MessageWithHeader {

  private final ManagedBuffer managedBuffer;
  private final ByteBuf header;
  private final int headerLength;
  private final Object body;
  private final long bodyLength;
  private long totalBytesTransferred;

  MessageWithHeader(ManagedBuffer managedBuffer, ByteBuf header, Object body, long bodyLength) {
    this.managedBuffer = managedBuffer;
    this.header = header;
    this.headerLength = header.readableBytes();
    this.body = body;
    this.bodyLength = bodyLength;
  }
}
