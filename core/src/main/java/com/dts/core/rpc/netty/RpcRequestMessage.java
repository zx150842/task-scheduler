package com.dts.core.rpc.netty;

import com.dts.core.rpc.RpcAddress;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class RpcRequestMessage implements Serializable {
  private static final long serialVersionUID = 3954306718552744594L;
  public final RpcAddress senderAddress;
  public final NettyRpcEndpointRef receiver;
  public final Object content;

  public RpcRequestMessage(RpcAddress senderAddress, NettyRpcEndpointRef receiver, Object content) {
    this.senderAddress = senderAddress;
    this.receiver = receiver;
    this.content = content;
  }
}
