package com.dts.rpc.netty;

import com.dts.rpc.RpcAddress;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.server.RpcHandler;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author zhangxin
 */
public class NettyRpcHandler extends RpcHandler {
  private final Logger logger = LoggerFactory.getLogger(NettyRpcHandler.class);

  private final Dispatcher dispatcher;
  private final NettyRpcEnv nettyRpcEnv;
  private final Map<RpcAddress, RpcAddress> remoteAddresses = Maps.newConcurrentMap();

  NettyRpcHandler(Dispatcher dispatcher, NettyRpcEnv nettyRpcEnv) {
    this.dispatcher = dispatcher;
    this.nettyRpcEnv = nettyRpcEnv;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    RpcRequestMessage messageToDispatch = internalReceive(client, message);
    dispatcher.postRemoteMessage(messageToDispatch, callback);
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message) {
    RpcRequestMessage messageToDispatch = internalReceive(client, message);
    dispatcher.postOneWayMessage(messageToDispatch);
  }

  private RpcRequestMessage internalReceive(TransportClient client, ByteBuffer message) {
    InetSocketAddress address = (InetSocketAddress) client.getChannel().remoteAddress();
    assert address != null;
    RpcAddress clientAddress = new RpcAddress(address.getHostString(), address.getPort());
    RpcRequestMessage requestMessage = (RpcRequestMessage) nettyRpcEnv.deserialize(client, message);
    if (requestMessage.senderAddress == null) {
      return new RpcRequestMessage(clientAddress, requestMessage.receiver, requestMessage.content);
    } else {
      RpcAddress remoteEnvAddress = requestMessage.senderAddress;
      if (remoteAddresses.putIfAbsent(clientAddress, remoteEnvAddress) == null) {
        dispatcher.postToAll(new RemoteProcessConnected(remoteEnvAddress));
      }
      return requestMessage;
    }
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    InetSocketAddress address = (InetSocketAddress) client.getChannel().remoteAddress();
    if (address != null) {
      RpcAddress clientAddr = new RpcAddress(address.getHostName(), address.getPort());
      dispatcher.postToAll(new RemoteProcessConnectionError(cause, clientAddr));
      RpcAddress remoteEnvAddress = remoteAddresses.get(clientAddr);
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(new RemoteProcessConnectionError(cause, remoteEnvAddress));
      }
    } else {
      logger.error("Exception before connecting to the client", cause);
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    InetSocketAddress address = (InetSocketAddress) client.getChannel().remoteAddress();
    assert address != null;
    RpcAddress clientAddr = new RpcAddress(address.getHostName(), address.getPort());
    dispatcher.postToAll(new RemoteProcessConnected(clientAddr));
  }

  @Override
  public void channelInactive(TransportClient client) {
    InetSocketAddress address = (InetSocketAddress) client.getChannel().remoteAddress();
    if (address != null) {
      RpcAddress clientAddr = new RpcAddress(address.getHostName(), address.getPort());
      nettyRpcEnv.removeOutbox(clientAddr);
      dispatcher.postToAll(new RemoteProcessDisconnected(clientAddr));
      RpcAddress remoteEnvAddress = remoteAddresses.remove(clientAddr);
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(new RemoteProcessDisconnected(remoteEnvAddress));
      }
    }
  }
}
