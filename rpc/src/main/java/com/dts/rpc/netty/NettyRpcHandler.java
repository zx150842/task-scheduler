package com.dts.rpc.netty;

import com.dts.rpc.MasterConf;
import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.server.RpcHandler;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author zhangxin
 */
public class NettyRpcHandler extends RpcHandler {
  private final Logger logger = LoggerFactory.getLogger(NettyRpcHandler.class);

  private final Dispatcher dispatcher;
  private final NettyRpcEnv nettyRpcEnv;

  NettyRpcHandler(Dispatcher dispatcher, NettyRpcEnv nettyRpcEnv) {
    this.dispatcher = dispatcher;
    this.nettyRpcEnv = nettyRpcEnv;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {

  }
}
