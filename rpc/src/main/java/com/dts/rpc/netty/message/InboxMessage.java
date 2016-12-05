package com.dts.rpc.netty.message;

import com.dts.rpc.RpcAddress;
import com.dts.rpc.netty.NettyRpcCallContext;

/**
 * @author zhangxin
 */
public class InboxMessage {
  public static class OnStart extends InboxMessage {}

  public static class OnStop extends InboxMessage {}

  public static class RemoteProcessConnected extends InboxMessage {
    public final RpcAddress remoteAddress;

    public RemoteProcessConnected(RpcAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }
  }

  public static class RemoteProcessDisconnected extends InboxMessage {
    public final RpcAddress remoteAddress;

    public RemoteProcessDisconnected(RpcAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }
  }

  public static class RemoteProcessConnectionError extends InboxMessage {
    public final Throwable cause;
    public final RpcAddress remoteAddress;

    public RemoteProcessConnectionError(Throwable cause, RpcAddress remoteAddress) {
      this.cause = cause;
      this.remoteAddress = remoteAddress;
    }
  }

  public static class OneWayInboxMessage extends InboxMessage {
    public RpcAddress senderAddress;
    public Object content;

    public OneWayInboxMessage(RpcAddress senderAddress, Object content) {
      this.senderAddress = senderAddress;
      this.content = content;
    }
  }

  public static class RpcInboxMessage extends InboxMessage {
    public final RpcAddress senderAddress;
    public final Object content;
    public final NettyRpcCallContext context;

    public RpcInboxMessage(RpcAddress senderAddress, Object content, NettyRpcCallContext context) {
      this.senderAddress = senderAddress;
      this.content = content;
      this.context = context;
    }
  }
}
