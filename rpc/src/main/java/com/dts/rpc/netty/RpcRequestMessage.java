package com.dts.rpc.netty;

import com.dts.rpc.RpcAddress;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class RpcRequestMessage implements Serializable {
  public final RpcAddress senderAddress;
  public final NettyRpcEndpointRef receiver;
  public final Object content;

  public RpcRequestMessage(RpcAddress senderAddress, NettyRpcEndpointRef receiver, Object content) {
    this.senderAddress = senderAddress;
    this.receiver = receiver;
    this.content = content;
  }
}
