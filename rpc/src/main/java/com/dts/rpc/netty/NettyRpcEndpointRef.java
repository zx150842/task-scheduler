package com.dts.rpc.netty;

import com.dts.rpc.*;
import com.dts.rpc.network.client.TransportClient;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * @author zhangxin
 */
public class NettyRpcEndpointRef extends RpcEndpointRef implements Serializable {
  private final Logger logger = LoggerFactory.getLogger(NettyRpcEndpointRef.class);

  private final RpcEndpointAddress endpointAddress;
  private final NettyRpcEnv nettyRpcEnv;
  private final RpcAddress address;
  private TransportClient client;

  public NettyRpcEndpointRef(DTSConf conf, RpcEndpointAddress endpointAddress,
      NettyRpcEnv nettyRpcEnv) {
    super(conf);
    this.endpointAddress = endpointAddress;
    this.nettyRpcEnv = nettyRpcEnv;
    this.address = endpointAddress.getRpcAddress();
  }

  @Override
  public RpcAddress address() {
    return endpointAddress.getRpcAddress();
  }

  @Override
  public String name() {
    return endpointAddress.getName();
  }

  @Override
  protected Logger logger() {
    return logger;
  }

  public TransportClient client() {
    return client;
  }

  public void send(Object message) {
    Preconditions.checkArgument(message != null, "Message is null");
    nettyRpcEnv.send(new RpcRequestMessage(nettyRpcEnv.address(), this, message));
  }

  public <T> Future<T> ask(T message, long timeoutMs) {
    RpcRequestMessage rpcMessage = new RpcRequestMessage(nettyRpcEnv.address(), this, message);
    return nettyRpcEnv.ask(rpcMessage, timeoutMs);
  }

  @Override
  public int hashCode() {
    if (address == null) {
      return 0;
    }
    return address.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof NettyRpcEndpointRef) {
      return address == ((NettyRpcEndpointRef) other).address;
    }
    return false;
  }
}
