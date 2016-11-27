package com.dts.rpc.network.server;

import com.dts.rpc.network.protocol.Message;

import java.io.IOException;

/**
 * @author zhangxin
 */
public abstract class MessageHandler<T extends Message> {

  public abstract void handle(T message) throws Exception;

  public abstract void channelActive();

  public abstract void exceptionCaught(Throwable cause);

  public abstract void channelInactive();
}
