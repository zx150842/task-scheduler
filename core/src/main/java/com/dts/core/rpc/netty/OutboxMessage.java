package com.dts.core.rpc.netty;

import com.google.common.util.concurrent.SettableFuture;

import com.dts.core.rpc.network.client.RpcResponseCallback;
import com.dts.core.rpc.network.client.TransportClient;
import com.dts.core.rpc.util.SerializerInstance;

import java.nio.ByteBuffer;

/**
 * @author zhangxin
 */
abstract class OutboxMessage {

  public abstract void sendWith(TransportClient client);

  public abstract void onFailure(Throwable e);
}

class OneWayOutboxMessage extends OutboxMessage {

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

class RpcOutboxMessage extends OutboxMessage implements RpcResponseCallback {

  private final SerializerInstance serializerInstance;
  private final ByteBuffer content;
  private final SettableFuture future;

  private TransportClient client;
  private long requestId;

  public RpcOutboxMessage(SerializerInstance serializerInstance, ByteBuffer content, SettableFuture future) {
    this.serializerInstance = serializerInstance;
    this.content = content;
    this.future = future;
  }

  @Override
  public void sendWith(TransportClient client) {
    this.client = client;
    this.requestId = client.sendRpc(content, this);
  }

  @Override
  public void onFailure(Throwable e) {
    future.setException(e);
  }

  @Override
  public void onSuccess(ByteBuffer response) {
    Object obj = serializerInstance.deserialize(response);
    if (obj instanceof Throwable) {
      future.setException((Throwable)obj);
    } else {
      future.set(obj);
    }
  }
}
