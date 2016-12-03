package com.dts.rpc.netty.message;

import com.dts.rpc.RpcAddress;
import com.dts.rpc.netty.NettyRpcEndpointRef;

/**
 * @author zhangxin
 */
public class RpcRequestMessage {
  public final RpcAddress senderAddress;
  public final NettyRpcEndpointRef receiver;
  public final Object content;

  public RpcRequestMessage(RpcAddress senderAddress, NettyRpcEndpointRef receiver, Object content) {
    this.senderAddress = senderAddress;
    this.receiver = receiver;
    this.content = content;
  }
}
