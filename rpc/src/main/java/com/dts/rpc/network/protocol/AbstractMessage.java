package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;

/**
 * @author zhangxin
 */
public abstract class AbstractMessage implements Message {

  private final ManagedBuffer body;

  protected AbstractMessage() {
    this(null);
  }

  protected  AbstractMessage(ManagedBuffer body) {
    this.body = body;
  }

  public ManagedBuffer body() {
    return body;
  }
}
