package com.dts.rpc.network.server;

import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.client.TransportResponseHandler;
import com.dts.rpc.network.protocol.Message;
import com.dts.rpc.network.protocol.RequestMessage;
import com.dts.rpc.network.protocol.ResponseMessage;
import com.dts.rpc.network.util.NettyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @author zhangxin
 */
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
    private final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

    private final TransportClient client;
    private final TransportRequestHandler requestHandler;
    private final TransportResponseHandler responseHandler;
    private final long requestTimeoutNs;
    private final boolean closeIdleConnections;

    public TransportChannelHandler(
            TransportClient client,
            TransportRequestHandler requestHandler,
            TransportResponseHandler responseHandler,
            long requestTimeoutNs,
            boolean closeIdleConnections
    ) {
        this.client = client;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.requestTimeoutNs = requestTimeoutNs * 1000L * 1000;
        this.closeIdleConnections = closeIdleConnections;
    }

    public TransportClient getClient() {
        return client;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.warn("Exception in connection from " + NettyUtils.getRemoteAddress(ctx.channel()), cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception{
        try {
            requestHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while register channel", e);
        }
        try {
            responseHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while register channel", e);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while unregister channel", e);
        }
        try {
            responseHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while unregister channel", e);
        }
        super.channelUnregistered(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message request) throws Exception {
        if (request instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) request);
        } else {
            responseHandler.handle((ResponseMessage) request);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;

            synchronized (this) {
                boolean isActuallyOverdue = System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    String address = NettyUtils.getRemoteAddress(ctx.channel());
                    logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
                            "requests. Assuming connection is dead; please adjust network.timeout if " +
                            "this is wrong.", address, requestTimeoutNs / 1000 / 1000);
                    client.timeOut();
                    ctx.close();
                } else if (closeIdleConnections) {
                    client.timeOut();
                    ctx.close();
                }

            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    public TransportResponseHandler getResponseHandler() {
        return responseHandler;
    }
}
