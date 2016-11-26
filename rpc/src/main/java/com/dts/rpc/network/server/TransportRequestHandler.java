package com.dts.rpc.network.server;

import com.dts.rpc.network.buffer.NioManagedBuffer;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.protocol.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {

  private final Channel channel;
  private final TransportClient reverseClient;
  private final RpcHandler rpcHandler;

  public TransportRequestHandler(Channel channel, TransportClient reverseClient,
    RpcHandler rpcHandler) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
  }

  @Override public void channelActive() {

  }

  @Override public void exceptionCaught(Throwable cause) {

  }

  @Override public void channelInactive() {

  }

  @Override public void handle(RequestMessage request) {
    if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    }
  }

  private void processRpcRequest(RpcRequest request) {

    try {
      rpcHandler.receive(reverseClient, request.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(request.requestId, new NioManagedBuffer(response)));
        }

        @Override public void onFailure(Throwable e) {
          respond(new RpcFailure(request.requestId, e.toString()));
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      request.body().release();
    }
  }

  private void respond(final Message result) {
    final String remoteAddress = channel.remoteAddress().toString();
    channel.writeAndFlush(result).addListener(new ChannelFutureListener() {
      @Override public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {

        } else {
          channel.close();
        }
      }
    });
  }
}
