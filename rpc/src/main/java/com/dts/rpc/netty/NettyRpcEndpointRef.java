package com.dts.rpc.netty;

import com.dts.rpc.*;
import com.dts.rpc.network.client.TransportClient;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * @author zhangxin
 */
public class NettyRpcEndpointRef extends RpcEndpointRef implements Serializable {
  private final Logger logger = LoggerFactory.getLogger(NettyRpcEndpointRef.class);

  private RpcEndpointAddress endpointAddress;
  transient private NettyRpcEnv nettyRpcEnv;
  private RpcAddress address;
  transient private TransportClient client;

  public NettyRpcEndpointRef() {}

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

  private void readObject(ObjectInputStream in) throws Exception {
    in.defaultReadObject();
    nettyRpcEnv = NettyRpcEnv.currentEnv();
    client = NettyRpcEnv.currentClient();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
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

  public <T> Future<T> ask(Object message) {
    RpcRequestMessage rpcMessage = new RpcRequestMessage(nettyRpcEnv.address(), this, message);
    return nettyRpcEnv.ask(rpcMessage);
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
      return address.equals(((NettyRpcEndpointRef) other).address);
    }
    return false;
  }
}
