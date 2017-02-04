package com.dts.core.rpc;

import com.dts.core.DTSConf;
import com.dts.core.rpc.netty.NettyRpcEnvFactory;
import com.dts.core.rpc.netty.RpcRequestMessage;
import com.dts.core.rpc.network.client.TransportClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zhangxin
 */
public abstract class RpcEnv {

  public static RpcEnv create(String name, String host, int port, DTSConf conf, boolean clientMode) {
    RpcEnvConfig rpcEnvConfig = new RpcEnvConfig(conf, name, host, port, clientMode);
    return new NettyRpcEnvFactory().create(rpcEnvConfig);
  }

  public abstract void startServer(int port);

  public abstract DTSConf conf();

  public abstract RpcAddress address();

  public abstract ThreadPoolExecutor clientConnectionExecutor();

  public abstract TransportClient createClient(RpcAddress address) throws IOException;

  public abstract void removeOutbox(RpcAddress address);

  public abstract RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint);

  public abstract RpcEndpointRef setupEndpointRef(RpcAddress address, String endpointName);

  public abstract ByteBuffer serialize(Object content) throws Exception;

  public abstract Object deserialize(TransportClient client, ByteBuffer bytes) throws Exception;

  public abstract RpcEndpointRef endpointRef(RpcEndpoint endpoint);

  public abstract void shutdown();

  public abstract void awaitTermination();

  public abstract void send(RpcRequestMessage message) throws Exception;

  public abstract <T> Future<T> ask(RpcRequestMessage message);

  public abstract void stop(RpcEndpointRef endpointRef);
}
