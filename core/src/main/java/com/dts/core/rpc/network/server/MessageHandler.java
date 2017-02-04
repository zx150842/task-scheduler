package com.dts.core.rpc.network.server;

import com.dts.core.rpc.network.protocol.Message;

/**
 * @author zhangxin
 */
public abstract class MessageHandler<T extends Message> {

  public abstract void handle(T message) throws Exception;

  public abstract void channelActive();

  public abstract void exceptionCaught(Throwable cause);

  public abstract void channelInactive();
}
