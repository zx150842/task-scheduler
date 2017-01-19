package com.dts.rpc.netty;

import com.dts.rpc.RpcAddress;
import com.dts.rpc.RpcCallContext;
import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * @author zhangxin
 */
abstract class InboxMessage implements Serializable {}

class OnStart extends InboxMessage {}

class OnStop extends InboxMessage {}

class RemoteProcessConnected extends InboxMessage {
  public final RpcAddress remoteAddress;

  public RemoteProcessConnected(RpcAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
  }
}

class RemoteProcessDisconnected extends InboxMessage {
  public final RpcAddress remoteAddress;

  public RemoteProcessDisconnected(RpcAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
  }
}

class RemoteProcessConnectionError extends InboxMessage {
  public final Throwable cause;
  public final RpcAddress remoteAddress;

  public RemoteProcessConnectionError(Throwable cause, RpcAddress remoteAddress) {
    this.cause = cause;
    this.remoteAddress = remoteAddress;
  }
}

class OneWayInboxMessage extends InboxMessage {
  public RpcAddress senderAddress;
  public Object content;

  public OneWayInboxMessage(RpcAddress senderAddress, Object content) {
    this.senderAddress = senderAddress;
    this.content = content;
  }
}

class RpcInboxMessage extends InboxMessage {
  public final RpcAddress senderAddress;
  public final Object content;
  public final RpcCallContext context;

  public RpcInboxMessage(RpcAddress senderAddress, Object content, RpcCallContext context) {
    this.senderAddress = senderAddress;
    this.content = content;
    this.context = context;
  }
}
