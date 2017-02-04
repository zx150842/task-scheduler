package com.dts.core.rpc.network.protocol;

import com.dts.core.rpc.network.buffer.ManagedBuffer;
import com.dts.core.rpc.network.buffer.NettyManagedBuffer;
import com.dts.core.rpc.network.util.ByteArrayWritableChannel;
import com.dts.core.rpc.network.util.TestManagedBuffer;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author zhangxin
 */
public class MessageWithHeaderTest {

  @Test
  public void testSingleWrite() throws Exception {
    testFileRegionBody(8, 8);
  }

  @Test
  public void testShortWrite() throws Exception {
    testFileRegionBody(8, 1);
  }

  @Test
  public void testByteBufBody() throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    ByteBuf bodyPassedToNettyManagedBuffer = Unpooled.copyLong(84);
    assertEquals(1, header.refCnt());
    assertEquals(1, bodyPassedToNettyManagedBuffer.refCnt());
    ManagedBuffer managedBuf = new NettyManagedBuffer(bodyPassedToNettyManagedBuffer);

    Object body = managedBuf.convertToNetty();
    assertEquals(2, bodyPassedToNettyManagedBuffer.refCnt());
    assertEquals(1, header.refCnt());

    MessageWithHeader msg = new MessageWithHeader(managedBuf, header, body, managedBuf.size());
    ByteBuf result = doWrite(msg, 1);
    assertEquals(msg.count(), result.readableBytes());
    assertEquals(42, result.readLong());
    assertEquals(84, result.readLong());

    assertTrue(msg.release());
    assertEquals(0, bodyPassedToNettyManagedBuffer.refCnt());
    assertEquals(0, header.refCnt());
  }

  @Test
  public void testDeallocateReleasesManagedBuffer() throws Exception {
    ByteBuf header = Unpooled.copyLong(12);
    ManagedBuffer managedBuf = Mockito.spy(new TestManagedBuffer(84));
    ByteBuf body = (ByteBuf)managedBuf.convertToNetty();
    assertEquals(2, body.refCnt());
    MessageWithHeader msg = new MessageWithHeader(managedBuf, header, body, body.readableBytes());
    assertTrue(msg.release());
    Mockito.verify(managedBuf, Mockito.times(1)).release();
    assertEquals(0, body.refCnt());
  }

  private void testFileRegionBody(int totalWrites, int writesPreCall) throws Exception {
    ByteBuf header = Unpooled.copyLong(42);
    int headerLength = header.readableBytes();
    TestFileRegion region = new TestFileRegion(totalWrites, writesPreCall);
    MessageWithHeader msg = new MessageWithHeader(null, header, region, region.count());

    ByteBuf result = doWrite(msg, totalWrites / writesPreCall);
    assertEquals(headerLength + region.count(), result.readableBytes());
    assertEquals(42, result.readLong());
    for (long i = 0; i < 8; ++i) {
      assertEquals(i, result.readLong());
    }
    assertTrue(msg.release());
  }

  private ByteBuf doWrite(MessageWithHeader msg, int minExpectedWrites) throws Exception {
    int writes = 0;
    ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int)msg.count());
    while (msg.transferred() < msg.count()) {
      msg.transferTo(channel, msg.transferred());
      writes++;
    }
    assertTrue("Not enough writes!", minExpectedWrites <= writes);
    return Unpooled.wrappedBuffer(channel.getData());
  }

  private static class TestFileRegion extends AbstractReferenceCounted implements FileRegion {

    private final int writeCount;
    private final int writesPerCall;
    private int written;

    TestFileRegion(int totalWrites, int writesPerCall) {
      this.writeCount = totalWrites;
      this.writesPerCall = writesPerCall;
    }

    @Override public long position() {
      return 0;
    }

    @Override public long transfered() {
      return 8 * written;
    }

    @Override public long transferred() {
      return 8 * written;
    }

    @Override public long count() {
      return 8 * writeCount;
    }

    @Override public long transferTo(WritableByteChannel target, long position) throws IOException {
      for (int i = 0; i < writesPerCall; ++i) {
        ByteBuf buf = Unpooled.copyLong((position / 8) + i);
        ByteBuffer nio = buf.nioBuffer();
        while (nio.remaining() > 0) {
          target.write(nio);
        }
        buf.release();
        written++;
      }
      return 8 * writesPerCall;
    }

    @Override
    public FileRegion touch() {
      return (FileRegion) super.touch();
    }

    @Override public FileRegion touch(Object hint) {
      return this;
    }

    @Override protected void deallocate() {

    }

    @Override
    public FileRegion retain() {
      return (FileRegion)super.retain();
    }

    @Override
    public FileRegion retain(int increment) {
      return (FileRegion) super.retain(increment);
    }
  }
}
