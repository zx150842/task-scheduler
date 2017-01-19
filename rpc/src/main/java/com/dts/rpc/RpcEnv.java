package com.dts.rpc;

import com.dts.rpc.netty.RpcRequestMessage;
import com.dts.rpc.network.client.TransportClient;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zhangxin
 */
public interface RpcEnv {

  void startServer(int port);

  DTSConf conf();

  RpcAddress address();

  ThreadPoolExecutor clientConnectionExecutor();

  TransportClient createClient(RpcAddress address) throws IOException;

  void removeOutbox(RpcAddress address);

  RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint);

  RpcEndpointRef setupEndpointRef(RpcAddress address, String endpointName);

  ByteBuffer serialize(Object content) throws Exception;

  Object deserialize(TransportClient client, ByteBuffer bytes) throws Exception;

  RpcEndpointRef endpointRef(RpcEndpoint endpoint);

  void shutdown();

  void awaitTermination();

  void send(RpcRequestMessage message) throws Exception;

  <T> Future<T> ask(RpcRequestMessage message);

  void stop(RpcEndpointRef endpointRef);
}
