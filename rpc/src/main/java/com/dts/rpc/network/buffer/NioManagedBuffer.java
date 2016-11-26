package com.dts.rpc.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public class NioManagedBuffer extends ManagedBuffer {
  private final ByteBuffer buf;

  public NioManagedBuffer(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override public long size() {
    return 0;
  }

  @Override public ByteBuffer nioByteBuffer() throws IOException {
    return null;
  }

  @Override public InputStream createInputStream() throws IOException {
    return null;
  }

  @Override public ManagedBuffer retain() {
    return null;
  }

  @Override public ManagedBuffer release() {
    return null;
  }

  @Override public Object convertToNetty() throws IOException {
    return null;
  }
}
