package com.dts.rpc.network.server;

import com.google.common.base.Throwables;

import com.dts.rpc.network.buffer.NioManagedBuffer;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.protocol.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
  private final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  private final Channel channel;
  private final TransportClient reverseClient;
  private final RpcHandler rpcHandler;

  public TransportRequestHandler(Channel channel, TransportClient reverseClient,
      RpcHandler rpcHandler) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
  }

  @Override
  public void channelActive() {
    rpcHandler.channelActive(reverseClient);
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    rpcHandler.exceptionCaught(cause, reverseClient);
  }

  @Override
  public void channelInactive() {
    rpcHandler.channelInactive(reverseClient);
  }

  @Override
  public void handle(RequestMessage request) {
    if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    } else if (request instanceof OneWayMessage) {
      processOneWayMessage((OneWayMessage) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processRpcRequest(RpcRequest request) {
    try {
      rpcHandler.receive(reverseClient, request.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(request.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(request.requestId, e.toString()));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + request.requestId, e);
      respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      request.body().release();
    }
  }

  private void processOneWayMessage(OneWayMessage request) {
    try {
      rpcHandler.receive(reverseClient, request.body().nioByteBuffer());
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() for one-way message", e);
    } finally {
      request.body().release();
    }
  }

  private void respond(final Message result) {
    final String remoteAddress = channel.remoteAddress().toString();
    channel.writeAndFlush(result).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          logger.trace("Sent result {} to client {}", result, remoteAddress);
        } else {
          logger.error("Error sending result {} to {}; closing connection", result, remoteAddress,
              future.cause());
          channel.close();
        }
      }
    });
  }
}
