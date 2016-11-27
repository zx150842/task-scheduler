package com.dts.rpc.network.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Created by zhangxin on 2016/11/27.
 */
public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {
    private final Logger logger = LoggerFactory.getLogger(MessageDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // bytebuf格式：1byte:type 8byte:requestId 4byte content length
        Message.Type msgType = Message.Type.decode(in);
        Message decoded = decode(msgType, in);
        assert decoded.type() == msgType;
        logger.trace("Received message " + msgType + ": " +decoded);
        out.add(decoded);

    }

    private Message decode(Message.Type msgType, ByteBuf in) {
        switch (msgType) {
            case RpcRequest:
                return RpcRequest.decode(in);
            case RpcResponse:
                return RpcResponse.decode(in);
            case RpcFailure:
                return RpcFailure.decode(in);
            default:
                throw new IllegalArgumentException("Unexpeted message type: " + msgType);
        }
    }
}
