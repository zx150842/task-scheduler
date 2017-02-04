package com.dts.core.rpc.util;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public class ByteBufferOutputStream extends ByteArrayOutputStream {
  private int capacity;

  public ByteBufferOutputStream() {
    this(32);
  }

  public ByteBufferOutputStream(int capacity) {
    this.capacity = capacity;
  }

  public int getCount() {
    return count;
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(buf, 0, count);
  }
}
