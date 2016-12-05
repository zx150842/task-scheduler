package com.dts.rpc.netty.message;

import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
public interface OutboxMessage {

  void sendWith(TransportClient client);

  void onFailure(Throwable e);

  class OneWayOutboxMessage implements OutboxMessage {

    private ByteBuffer content;

    public OneWayOutboxMessage(ByteBuffer content) {
      this.content = content;
    }

    @Override
    public void sendWith(TransportClient client) {
      client.send(content);
    }

    @Override
    public void onFailure(Throwable e) {

    }
  }


  class RpcOutboxMessage implements OutboxMessage {

    private final ByteBuffer content;
    private final RpcResponseCallback callback;

    private TransportClient client;
    private long requestId;

    public RpcOutboxMessage(ByteBuffer content, RpcResponseCallback callback) {
      this.content = content;
      this.callback = callback;
    }

    @Override
    public void sendWith(TransportClient client) {
      this.client = client;
      this.requestId = client.sendRpc(content, callback);
    }

    @Override
    public void onFailure(Throwable e) {
      callback.onFailure(e);
    }

    public void onSuccess(ByteBuffer response) {
      callback.onSuccess(response);
    }

  }

}
