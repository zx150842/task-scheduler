package com.dts.core.rpc.network.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * 对发送到网络的消息编码
 * <p>一个完整包的结构：
 * <p>| header | message body |
 * <p>其中header结构如下：
 * <p>| frame length | message type | message meta |
 * <p>其中message meta结构如下(有的消息类型可能没有requestId或没有body size其中之一)：
 * <p>| requestId | body size |
 *
 * @author zhangxin
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {
  private final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  @Override
  protected void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    Object body = null;
    long bodyLength = 0;
    // 包中是否包含了message body
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
    // 包头由8位的frame length + message type + message meta组成
    int headerLength = 8 + msgType.encodeLength() + in.encodeLength();
    // 包的总长度为header length + body length(如果包含了message body)
    long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
    // 写入header信息
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    header.writeLong(frameLength);
    msgType.encode(header);
    in.encode(header);
    assert header.writableBytes() == 0;

    // 如果body不空则返回MessageWithHeader
    if (body != null) {
      out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
    } else {
      out.add(header);
    }
  }
}
