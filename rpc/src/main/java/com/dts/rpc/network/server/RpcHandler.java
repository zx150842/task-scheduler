package com.dts.rpc.network.server;

import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public abstract class RpcHandler {

  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();

  public abstract void receive(TransportClient client, ByteBuffer message,
      RpcResponseCallback callback) throws Exception;

  public void receive(TransportClient client, ByteBuffer message) throws Exception {
    receive(client, message, ONE_WAY_CALLBACK);
  }

  public void channelActive(TransportClient client) {}

  public void channelInactive(TransportClient client) {}

  public void exceptionCaught(Throwable cause, TransportClient client) {}

  private static class OneWayRpcCallback implements RpcResponseCallback {
    private final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

    @Override public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC");
    }

    @Override public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC", e);
    }
  }
}
