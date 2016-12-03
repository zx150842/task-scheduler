package com.dts.rpc.util;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * @author zhangxin
 */
public class ByteBufferInputStream extends InputStream {

  private ByteBuffer buffer;
  private boolean dispose;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this(buffer, false);
  }

  public ByteBufferInputStream(ByteBuffer buffer, boolean dispose) {
    this.buffer = buffer;
    this.dispose = dispose;
  }


  @Override
  public int read() throws IOException {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp();
      return -1;
    } else {
      return buffer.get();
    }
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp();
      return -1;
    } else {
      int amountToGet = Math.min(len, buffer.remaining());
      buffer.get(b, off, amountToGet);
      return amountToGet;
    }
  }

  @Override
  public long skip(long bytes) throws IOException {
    if (buffer != null) {
      int amoutToSkip = (int) Math.min(bytes, buffer.remaining());
      buffer.position(buffer.position() + amoutToSkip);
      if (buffer.remaining() == 0) {
        cleanUp();
      }
      return amoutToSkip;
    } else {
      return 0L;
    }
  }

  private void cleanUp() {
    if (buffer != null) {
      if (dispose && buffer instanceof MappedByteBuffer) {
        DirectBuffer directBuffer = (DirectBuffer) buffer;
        if (directBuffer.cleaner() != null) {
          directBuffer.cleaner().clean();
        }
      }
      buffer = null;
    }
  }
}
