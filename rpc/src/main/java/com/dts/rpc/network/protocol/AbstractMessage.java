package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;

import java.util.Objects;

/**
 * @author zhangxin
 */
public abstract class AbstractMessage implements Message {

  private final ManagedBuffer body;
  private final boolean isBodyInFrame;

  protected AbstractMessage() {
    this(null, false);
  }

  protected AbstractMessage(ManagedBuffer body, boolean isBodyInFrame) {
    this.body = body;
    this.isBodyInFrame = isBodyInFrame;
  }

  @Override
  public ManagedBuffer body() {
    return body;
  }

  @Override
  public boolean isBodyInFrame() {
    return isBodyInFrame;
  }

  protected boolean equals(AbstractMessage other) {
    return isBodyInFrame == other.isBodyInFrame && Objects.equals(body, other.body);
  }
}
