package com.dts.core.rpc.network.server;

import com.dts.core.rpc.network.client.RpcResponseCallback;
import com.dts.core.rpc.network.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public class NoOpRpcHandler extends RpcHandler {

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    throw new UnsupportedOperationException("Cannot handle messages");
  }

  @Override public void receive(TransportClient client, ByteBuffer message) {
    throw new UnsupportedOperationException("Cannot handle messages");
  }
}
