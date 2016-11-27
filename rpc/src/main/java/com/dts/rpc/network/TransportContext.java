package com.dts.rpc.network;

import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.client.TransportClientFactory;
import com.dts.rpc.network.client.TransportResponseHandler;
import com.dts.rpc.network.protocol.MessageDecoder;
import com.dts.rpc.network.protocol.MessageEncoder;
import com.dts.rpc.network.server.RpcHandler;
import com.dts.rpc.network.server.TransportChannelHandler;
import com.dts.rpc.network.server.TransportRequestHandler;
import com.dts.rpc.network.server.TransportServer;
import com.dts.rpc.network.util.TransportConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * @author zhangxin
 */
public class TransportContext {
    private final Logger logger = LoggerFactory.getLogger(TransportContext.class);

    private final TransportConf conf;
    private final RpcHandler rpcHandler;
    private final boolean closeIdleConnections;

    private final MessageEncoder encoder;
    private final MessageDecoder decoder;

    public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
        this(conf, rpcHandler, false);
    }

    public TransportContext(
            TransportConf conf,
            RpcHandler rpcHandler,
            boolean closeIdleConnections
    ) {
        this.conf = conf;
        this.rpcHandler = rpcHandler;
        this.closeIdleConnections = closeIdleConnections;
        this.encoder = new MessageEncoder();
        this.decoder = new MessageDecoder();
    }

    public TransportClientFactory createClientFactory() {
        return new TransportClientFactory(this);
    }

    public TransportServer createServer(int port) {
        return new TransportServer(this, null, port, rpcHandler);
    }

    public TransportServer createServer(String host, int port) {
        return new TransportServer(this, host, port, rpcHandler);
    }

    public TransportConf getConf() {
        return conf;
    }

    public TransportChannelHandler initializePipeline(SocketChannel channel) {
        return initializePipeline(channel, rpcHandler);
    }

    public TransportChannelHandler initializePipeline(
            SocketChannel channel,
            RpcHandler channelRpcHandler) {
        try {
            TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
            channel.pipeline()
                    .addLast("encoder", encoder)
                    .addLast("decoder", decoder)
                    .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (RuntimeException e) {
            logger.error("Error while initializing Netty pipeline", e);
            throw e;
        }
    }

    private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client, rpcHandler);
        return new TransportChannelHandler(client, requestHandler, responseHandler, conf.connectionTimeoutMs(), closeIdleConnections);
    }
}
