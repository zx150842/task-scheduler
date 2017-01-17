package com.dts.rpc.network;

import com.dts.rpc.network.protocol.*;
import com.dts.rpc.network.util.ByteArrayWritableChannel;
import com.dts.rpc.network.util.TestManagedBuffer;
import com.dts.rpc.network.util.TransportFrameDecoder;
import com.google.common.primitives.Ints;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author zhangxin
 */
public class ProtocolTest {

  private void testServerClient(Message msg) {
    EmbeddedChannel outChannel = new EmbeddedChannel(new FileRegionEncoder(),
      new MessageEncoder());
    outChannel.writeOutbound(msg);

    EmbeddedChannel inChannel = new EmbeddedChannel(new TransportFrameDecoder(),
      new MessageDecoder());

    while (!outChannel.outboundMessages().isEmpty()) {
      inChannel.writeInbound((Object) outChannel.readOutbound());
    }
    assertEquals(1, inChannel.inboundMessages().size());
    assertEquals(msg, inChannel.readInbound());
  }

  @Test
  public void requests() {
    testServerClient(new RpcRequest(12345, new TestManagedBuffer(0)));
    testServerClient(new RpcRequest(12345, new TestManagedBuffer(10)));
  }

  @Test
  public void responses() {
    testServerClient(new RpcResponse(12345, new TestManagedBuffer(0)));
    testServerClient(new RpcResponse(12345, new TestManagedBuffer(10)));
    testServerClient(new RpcFailure(0, "this is an error"));
    testServerClient(new RpcFailure(0, ""));
  }

  private static class FileRegionEncoder extends MessageToMessageEncoder<FileRegion> {

    @Override protected void encode(ChannelHandlerContext ctx, FileRegion msg, List<Object> out)
      throws Exception {
      ByteArrayWritableChannel channel = new ByteArrayWritableChannel(Ints.checkedCast(msg.count()));
      while (msg.transferred() < msg.count()) {
        msg.transferTo(channel, msg.transferred());
      }
      out.add(Unpooled.wrappedBuffer(channel.getData()));
    }
  }
}
