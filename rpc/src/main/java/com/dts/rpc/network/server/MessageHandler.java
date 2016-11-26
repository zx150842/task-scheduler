package com.dts.rpc.network.server;

import com.dts.rpc.network.protocol.Message;

/**
 * @author zhangxin
 */
public abstract class MessageHandler<T extends Message> {

  public abstract void handle(T message);

  public abstract void channelActive();

  public abstract void exceptionCaught(Throwable cause);

  public abstract void channelInactive();
}
