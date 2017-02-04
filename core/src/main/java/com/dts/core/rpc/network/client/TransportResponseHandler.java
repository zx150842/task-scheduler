package com.dts.core.rpc.network.client;

import com.dts.core.rpc.network.protocol.ResponseMessage;
import com.dts.core.rpc.network.protocol.RpcFailure;
import com.dts.core.rpc.network.protocol.RpcResponse;
import com.dts.core.rpc.network.server.MessageHandler;
import com.dts.core.rpc.network.util.NettyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;

/**
 * @author zhangxin
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;
  private final Map<Long, RpcResponseCallback> outstandingRpcs;

  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingRpcs = new ConcurrentHashMap<>();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    updateTimeOfLastRequest();
    outstandingRpcs.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      entry.getValue().onFailure(cause);
    }
    outstandingRpcs.clear();
  }

  @Override
  public void channelActive() {}

  @Override
  public void channelInactive() {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  @Override
  public void handle(ResponseMessage message) throws Exception {
    String remoteAddress = NettyUtils.getRemoteAddress(channel);
    if (message instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
            resp.requestId, remoteAddress, resp.body().size());
      } else {
        outstandingRpcs.remove(resp.requestId);
        try {
          listener.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
            resp.requestId, remoteAddress, resp.errorString);
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  public int numOutstandingRequests() {
    return outstandingRpcs.size();
  }

  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }
}
