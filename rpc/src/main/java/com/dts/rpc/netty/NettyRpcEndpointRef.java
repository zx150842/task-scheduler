package com.dts.rpc.netty;

import com.dts.rpc.*;
import com.dts.rpc.network.client.TransportClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zhangxin
 */
public class NettyRpcEndpointRef extends RpcEndpointRef {
  private final Logger logger = LoggerFactory.getLogger(NettyRpcEndpointRef.class);

  private final RpcEndpointAddress endpointAddress;
  private final NettyRpcEnv nettyRpcEnv;
  private TransportClient client;

  public NettyRpcEndpointRef(DTSConf conf, RpcEndpointAddress endpointAddress,
      NettyRpcEnv nettyRpcEnv) {
    super(conf);
    this.endpointAddress = endpointAddress;
    this.nettyRpcEnv = nettyRpcEnv;
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

  public TransportClient client() { return client; }

  public void send(Object message) {

  }

  public <T> Future<T> ask(T message, long timeoutMs) {
    return null;
  }

}
