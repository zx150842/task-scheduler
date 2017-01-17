package com.dts.rpc.network.client;

import com.dts.rpc.network.protocol.OneWayMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

import com.dts.rpc.network.buffer.NioManagedBuffer;
import com.dts.rpc.network.protocol.RpcRequest;
import com.dts.rpc.network.util.NettyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * @author zhangxin
 */
public class TransportClient implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;
  private String clientId;
  private boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this(channel, handler, false);
  }

  public TransportClient(Channel channel, TransportResponseHandler handler, boolean timedOut) {
    this.channel = channel;
    this.handler = handler;
    this.timedOut = timedOut;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String id) {
    this.clientId = id;
  }

  public long sendRpc(ByteBuffer message, final RpcResponseCallback callback) {
    final String serverAddr = NettyUtils.getRemoteAddress(channel);
    final long startTime = System.currentTimeMillis();
    logger.trace("Sending RPC to {}", serverAddr);

    final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    handler.addRpcRequest(requestId, callback);

    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
        .addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              logger.trace("Sending request {} to {} took {} ms", requestId, serverAddr,
                  System.currentTimeMillis() - startTime);
            } else {
              String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                  serverAddr, future.cause());
              logger.error(errorMsg, future.cause());
              handler.removeRpcRequest(requestId);
              channel.close();
              try {
                callback.onFailure(new IOException(errorMsg, future.cause()));
              } catch (Exception e) {
                logger.error("Uncaught exception in RPC response callback handler!", e);
              }
            }
          }
        });
    return requestId;
  }

  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    final SettableFuture<ByteBuffer> result = SettableFuture.create();
    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        ByteBuffer copy = ByteBuffer.allocate(response.remaining());
        copy.put(response);
        copy.flip();
        result.set(copy);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });
    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      throw new RuntimeException(e.getCause());
    }
  }

  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() throws IOException {
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("remoteAddress", channel.remoteAddress())
        .add("clientId", clientId).add("isActive", isActive()).toString();
  }
}
