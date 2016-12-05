package com.dts.rpc.netty;

import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.RpcEndpointAddress;
import com.dts.rpc.RpcEndpointRef;
import com.dts.rpc.netty.message.InboxMessage;
import com.dts.rpc.netty.message.RpcRequestMessage;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class Dispatcher {
  private final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

  private final NettyRpcEnv nettyRpcEnv;

  private final Map<String, EndpointData> endpoints = Maps.newConcurrentMap();
  private final Map<RpcEndpoint, NettyRpcEndpointRef> endpointRefs = Maps.newConcurrentMap();

  private final BlockingQueue<EndpointData> receivers = Queues.newLinkedBlockingQueue();

  private final ThreadPoolExecutor threadPool;

  private final boolean stopped = false;

  public Dispatcher(NettyRpcEnv nettyRpcEnv) {

    this.nettyRpcEnv = nettyRpcEnv;

    int numThreads = nettyRpcEnv.conf().getInt("master.rpc.netty.dispatcher.numThreads",
      Math.max(2, Runtime.getRuntime().availableProcessors()));
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("dispatcher-event-loop-%d").build();
    this.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads, threadFactory);
    for (int i = 0; i < numThreads; ++i) {
      threadPool.execute(new MessageLoop());
    }
  }

  private class MessageLoop implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          EndpointData data = receivers.take();
          data.inbox.process(Dispatcher.this);
          // TODO process data
        } catch (InterruptedException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }
  }

  public NettyRpcEndpointRef registerRpcEndpoint(String name, RpcEndpoint endpoint) {
    RpcEndpointAddress address = new RpcEndpointAddress(nettyRpcEnv.address(), name);
    NettyRpcEndpointRef endpointRef = new NettyRpcEndpointRef(nettyRpcEnv.conf(), address, nettyRpcEnv);
    synchronized (this) {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped");
      }
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException("There is already an RpcEndpoint called " + name);
      }
      EndpointData data = endpoints.get(name);
      endpointRefs.put(data.endpoint, data.ref);
      receivers.offer(data);
    }
    return endpointRef;
  }

  public void unregisterRpcEndpoint(String name) {
    EndpointData data = endpoints.remove(name);
    if (data != null) {
      //TODO stop inbox
      receivers.offer(data);
    }
  }

  public void stop() {
    synchronized (this) {
      if (stopped) { return; }
      for (String name : endpoints.keySet()) {
        unregisterRpcEndpoint(name);
        threadPool.shutdown();
      }
    }
  }

  public RpcEndpointRef getRpcEndpointRef(RpcEndpoint endpoint) {
    return endpointRefs.get(endpoint);
  }

  public void removeRpcEndpointRef(RpcEndpoint endpoint) {
    endpointRefs.remove(endpoint);
  }

  public void postToAll(InboxMessage message) {
    for (String name : endpoints.keySet()) {
      postMessage(name, message);
    }
  }

  public void postOneWayMessage(RpcRequestMessage message) {
    postMessage(message.receiver.name(), new InboxMessage.AskInboxMessage(message.senderAddress, message.content));
  }

  public void postLocalMessage(RpcRequestMessage message) {
    NettyRpcCallContext rpcCallContext = new LocalNettyRpcCallContext(message);
    InboxMessage.AskReplyInboxMessage rpcMessage = new InboxMessage.AskReplyInboxMessage(message.senderAddress, message.content, rpcCallContext);
    postMessage(message.receiver.name(), rpcMessage);
  }

  public void postRemoteMessage(RpcRequestMessage message, RpcResponseCallback callback) {
    NettyRpcCallContext rpcCallContext = new RemoteNettyRpcCallContext(nettyRpcEnv, callback, message.senderAddress);
    InboxMessage.AskReplyInboxMessage rpcMessage = new InboxMessage.AskReplyInboxMessage(message.senderAddress, message.content, rpcCallContext);
    postMessage(message.receiver.name(), rpcMessage);
  }

  private void postMessage(String endpointName, InboxMessage message) {
    synchronized (this) {
      EndpointData data = endpoints.get(endpointName);
      if (stopped) {

      } else if (data == null) {

      } else {
        data.inbox.post(message);
        receivers.offer(data);
      }
    }
  }

  public void awaitTermination() {
    try {
      threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  class EndpointData {
    String name;
    RpcEndpoint endpoint;
    NettyRpcEndpointRef ref;
    final Inbox inbox;

    EndpointData(final String name, final RpcEndpoint endpoint, final NettyRpcEndpointRef ref) {
      this.name = name;
      this.endpoint = endpoint;
      this.ref = ref;
      this.inbox = new Inbox(ref, endpoint);
    }
  }
}
