package com.dts.core.rpc.network.protocol;

import com.google.common.base.Preconditions;

import com.dts.core.rpc.network.buffer.ManagedBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

/**
 * @author zhangxin
 */
public class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

  private final ManagedBuffer managedBuffer;
  private final ByteBuf header;
  private final int headerLength;
  private final Object body;
  private final long bodyLength;
  private long totalBytesTransferred;

  private static final int NIO_BUFFER_LIMIT = 256 * 1024;

  MessageWithHeader(ManagedBuffer managedBuffer, ByteBuf header, Object body, long bodyLength) {
    this.managedBuffer = managedBuffer;
    this.header = header;
    this.headerLength = header.readableBytes();
    this.body = body;
    this.bodyLength = bodyLength;
  }

  @Override
  public long position() {
    return 0;
  }

  @Override
  public long transfered() {
    return totalBytesTransferred;
  }

  @Override
  public long transferred() {
    return totalBytesTransferred;
  }

  @Override
  public long count() {
    return headerLength + bodyLength;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
    Preconditions.checkArgument(position == totalBytesTransferred, "Invalid position");
    long writtenHeader = 0;
    if (header.readableBytes() > 0) {
      writtenHeader = copyByteBuf(header, target);
      totalBytesTransferred += writtenHeader;
      if (header.readableBytes() > 0) {
        return writtenHeader;
      }
    }

    long writtenBody = 0;
    if (body instanceof FileRegion) {
      writtenBody = ((FileRegion) body).transferTo(target, totalBytesTransferred - headerLength);
    } else if (body instanceof ByteBuf) {
      writtenBody = copyByteBuf((ByteBuf) body, target);
    }
    totalBytesTransferred += writtenBody;
    return writtenHeader + writtenBody;
  }

  @Override
  public FileRegion touch() {
    return this;
  }

  @Override
  public FileRegion touch(Object hint) {
    return this;
  }

  @Override
  protected void deallocate() {
    header.release();
    ReferenceCountUtil.release(body);
    if (managedBuffer != null) {
      managedBuffer.release();
    }
  }

  @Override
  public FileRegion retain() {
    super.retain();
    return this;
  }

  @Override
  public FileRegion retain(int increment) {
    super.retain(increment);
    return this;
  }

  private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
    ByteBuffer buffer = buf.nioBuffer();
    int written = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? target.write(buffer)
        : writeNioBuffer(target, buffer);
    buf.skipBytes(written);
    return written;
  }

  private int writeNioBuffer(WritableByteChannel writeCh, ByteBuffer buf) throws IOException {
    int originalLimit = buf.limit();
    int ret = 0;

    try {
      int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
      buf.limit(buf.position() + ioSize);
      ret = writeCh.write(buf);
    } finally {
      buf.limit(originalLimit);
    }
    return ret;
  }
}
