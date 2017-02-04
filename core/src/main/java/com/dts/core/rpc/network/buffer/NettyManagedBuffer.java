package com.dts.core.rpc.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

/**
 * Created by zhangxin on 2016/11/27.
 */
public class NettyManagedBuffer extends ManagedBuffer {
  private final ByteBuf buf;

  public NettyManagedBuffer(ByteBuf buf) {
    this.buf = buf;
  }

  @Override
  public long size() {
    return buf.readableBytes();
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    return buf.nioBuffer();
  }

  @Override
  public InputStream createInputStream() throws IOException {
    return new ByteBufInputStream(buf);
  }

  @Override
  public ManagedBuffer retain() {
    buf.retain();
    return this;
  }

  @Override
  public ManagedBuffer release() {
    buf.release();
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    return buf.duplicate().retain();
  }

  @Override
  public String toString() {
    return "";
  }
}
