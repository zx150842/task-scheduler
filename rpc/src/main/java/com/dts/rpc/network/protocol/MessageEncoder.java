package com.dts.rpc.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhangxin
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {
  private final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  @Override
  protected void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    Object body = null;
    long bodyLength = 0;
    boolean isBodyInFrame = false;

    if (in.body() != null) {
      try {
        bodyLength = in.body().size();
        body = in.body().convertToNetty();
        isBodyInFrame = in.isBodyInFrame();
      } catch (Exception e) {
        in.body().release();
        if (in instanceof RpcResponse) {
          RpcResponse resp = (RpcResponse) in;
          String error = e.getMessage() != null ? e.getMessage() : "null";
          logger.error("Error processing {} for client {}", in, ctx.channel().remoteAddress(), e);
          encode(ctx, new RpcFailure(resp.requestId, error), out);
        } else {
          throw e;
        }
        return;
      }
    }

    Message.Type msgType = in.type();
    int headerLength = 8 + msgType.encodeLength() + in.encodeLength();
    long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    header.writeLong(frameLength);
    msgType.encode(header);
    in.encode(header);
    assert header.writableBytes() == 0;

    if (body != null) {
      out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
    } else {
      out.add(header);
    }
  }
}
