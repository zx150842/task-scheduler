package com.dts.rpc.network.server;

import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public abstract class RpcHandler {

  public abstract void receive(TransportClient client, ByteBuffer message,
      RpcResponseCallback callback);

  public void channelActive(TransportClient client) {}

  public void channelInactive(TransportClient client) {}

  public void exceptionCaught(Throwable cause, TransportClient client) {}
}
