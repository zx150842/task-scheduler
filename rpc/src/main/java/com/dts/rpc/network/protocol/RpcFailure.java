package com.dts.rpc.network.protocol;

/**
 * @author zhangxin
 */
public class RpcFailure extends AbstractMessage {
  public final long requestId;
  public final String errorString;

  public RpcFailure(long requestId, String errorString) {
    this.requestId = requestId;
    this.errorString = errorString;
  }
}
