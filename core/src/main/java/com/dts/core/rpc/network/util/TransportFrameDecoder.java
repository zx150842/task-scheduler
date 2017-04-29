package com.dts.core.rpc.network.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.LinkedList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 这个类完成TCP粘包/拆包过程，实现从网络接收字节流并组装成一个个完整的包，提供给下游handler
 *
 * <p>一个完整包的结构：
 * <p>| header | message body |
 * <p>其中header结构如下：
 * <p>| frame length | message type | message meta |
 * <p>其中message meta结构如下：
 * <p>| requestId | body size |
 *
 * @author zhangxin
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

  public static final String HANDLER_NAME = "frameDecoder";
  private static final int LENGTH_SIZE = 8;
  private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
  private static final int UNKNOWN_FRAME_SIZE = -1;

  private final LinkedList<ByteBuf> buffers = Lists.newLinkedList();
  private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);

  // 从网络接收到的字节数
  private long totalSize = 0;
  // 一个完整包除去frame length之外的大小
  private long nextFrameSize = UNKNOWN_FRAME_SIZE;
  private volatile  Interceptor interceptor;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    ByteBuf in = (ByteBuf) data;
    buffers.add(in);
    totalSize += in.readableBytes();

    while (!buffers.isEmpty()) {
      if (interceptor != null) {
        ByteBuf first = buffers.getFirst();
        int available = first.readableBytes();
        if (feedInterceptor(first)) {
          assert !first.isReadable() : "Interceptor still active but buffer has data.";
        }

        int read = available - first.readableBytes();
        if (read == available) {
          buffers.removeFirst().release();
        }
        totalSize -= read;
      } else {
        ByteBuf frame = decodeNext();
        if (frame == null) {
          break;
        }
        ctx.fireChannelRead(frame);
      }
    }
  }

  private long decodeFrameSize() {
    if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
      return nextFrameSize;
    }

    ByteBuf first = buffers.getFirst();
    // 如果收到包的大小大于8个字节（即包含完整的frame length）
    if (first.readableBytes() >= LENGTH_SIZE) {
      // 直接读取frame length（包长度），并算出除了frame length之外的包大小
      nextFrameSize = first.readLong() - LENGTH_SIZE;
      totalSize -= LENGTH_SIZE;
      if (!first.isReadable()) {
        buffers.removeFirst().release();
      }
      return nextFrameSize;
    }

    // 如果收到的字节数不足8个（即出现半包的情况），则需要等待直到读到完整的包
    while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
      ByteBuf next = buffers.getFirst();
      int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
      frameLenBuf.writeBytes(next, toRead);
      // 如果当前包已经读完则直接删掉
      if (!next.isReadable()) {
        buffers.removeFirst().release();
      }
    }

    nextFrameSize = frameLenBuf.readLong() - LENGTH_SIZE;
    totalSize -= LENGTH_SIZE;
    frameLenBuf.clear();
    return nextFrameSize;
  }

  private ByteBuf decodeNext() throws Exception {
    long frameSize = decodeFrameSize();
    if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
      return null;
    }

    nextFrameSize = UNKNOWN_FRAME_SIZE;

    Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE, "Too large frame: %s", frameSize);
    Preconditions.checkArgument(frameSize > 0, "Frame length should be positive: %s", frameSize);

    int remaining = (int) frameSize;
    if (buffers.getFirst().readableBytes() >= remaining) {
      return nextBufferForFrame(remaining);
    }

    CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
    while (remaining > 0) {
      ByteBuf next = nextBufferForFrame(remaining);
      remaining -= next.readableBytes();
      frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
    }
    assert remaining == 0;
    return frame;
  }

  private ByteBuf nextBufferForFrame(int bytesToRead) {
    ByteBuf buf = buffers.getFirst();
    ByteBuf frame;

    if (buf.readableBytes() > bytesToRead) {
      frame = buf.retain().readSlice(bytesToRead);
      totalSize -= bytesToRead;
    } else {
      frame = buf;
      buffers.removeFirst();
      totalSize -= frame.readableBytes();
    }
    return frame;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    for (ByteBuf b : buffers) {
      b.release();
    }
    if (interceptor != null) {
      interceptor.channelInactive();
    }
    frameLenBuf.release();
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (interceptor != null) {
      interceptor.exceptionCaught(cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  public void setInterceptor(Interceptor interceptor) {
    this.interceptor = interceptor;
  }

  private boolean feedInterceptor(ByteBuf buf) throws Exception {
    if (interceptor != null && !interceptor.handle(buf)) {
      interceptor = null;
    }
    return interceptor != null;
  }

  public interface Interceptor {
    boolean handle(ByteBuf data) throws Exception;
    void exceptionCaught(Throwable cause) throws Exception;
    void channelInactive() throws Exception;
  }
}
