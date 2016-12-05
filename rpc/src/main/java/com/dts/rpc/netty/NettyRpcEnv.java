package com.dts.rpc.netty;

import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcAddress;
import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.RpcEndpointRef;
import com.dts.rpc.netty.message.InboxMessage;
import com.dts.rpc.netty.message.OutboxMessage;
import com.dts.rpc.netty.message.RpcRequestMessage;
import com.dts.rpc.network.TransportContext;
import com.dts.rpc.network.client.RpcResponseCallback;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.client.TransportClientFactory;
import com.dts.rpc.network.protocol.RpcFailure;
import com.dts.rpc.network.server.TransportServer;
import com.dts.rpc.network.util.NettyUtils;
import com.dts.rpc.network.util.TransportConf;
import com.dts.rpc.util.SerializerInstance;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.commons.lang3.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;

/**
 * @author zhangxin
 */
public class NettyRpcEnv {
  private final Logger logger = LoggerFactory.getLogger(NettyRpcEnv.class);

  private final String host;
  private final TransportConf transportConf;
  private final Dispatcher dispatcher;
  private final TransportContext transportContext;
  private final TransportClientFactory clientFactory;
  private final SerializerInstance serializerInstance;

  private final DTSConf conf;

  private TransportServer server;

  private final ThreadPoolExecutor clientConnectionExecutor;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private final Map<RpcAddress, Outbox> outboxes = Maps.newConcurrentMap();

  private RpcAddress address = null;

  NettyRpcEnv(DTSConf conf, SerializerInstance serializerInstance, String host) {
    this.host = host;
    this.conf = conf;
    this.serializerInstance = serializerInstance;
    this.transportConf = conf.getTransportConf("rpc");
    this.dispatcher = new Dispatcher(this);
    this.transportContext =
        new TransportContext(transportConf, new NettyRpcHandler(dispatcher, this));
    this.clientConnectionExecutor = NettyUtils.newDaemonCachedThreadPool("netty-rpc-connection",
        conf.getInt("dts.rpc.connect.threads", 64), 60);
    this.clientFactory = transportContext.createClientFactory();
  }

  public void startServer(int port) {
    server = transportContext.createServer(host, port);
    // TODO register to dispatcher
  }

  public DTSConf conf() {
    return conf;
  }

  public RpcAddress address() {
    if (server != null && address == null) {
      address = new RpcAddress(host, server.getPort());
    }
    return address;
  }

  public ThreadPoolExecutor clientConnectionExecutor() { return clientConnectionExecutor; }

  public TransportClient createClient(RpcAddress address) throws IOException {
    return clientFactory.createClient(new InetSocketAddress(address.getHost(), address.getPort()));
  }

  public void removeOutbox(RpcAddress address) {
    Outbox outbox = outboxes.remove(address);
    if (outbox != null) {
      outbox.stop();
    }
  }

  public ByteBuffer serialize(Object content) {
    return serializerInstance.serialize(content);
  }

  public <T> T deserialize(TransportClient client, ByteBuffer bytes) {
    return serializerInstance.deserialize(bytes);
  }

  public RpcEndpointRef endpointRef(RpcEndpoint endpoint) {
    return dispatcher.getRpcEndpointRef(endpoint);
  }

  public void shutdown() {
    try {
      cleanup();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void awaitTermination() {
    dispatcher.awaitTermination();
  }

  public void send(RpcRequestMessage message) {
    RpcAddress remoteAddr = message.receiver.address();
    if (remoteAddr == address()) {
      dispatcher.postOneWayMessage(message);
    } else {
      postToOutbox(message.receiver, new OutboxMessage.OneWayOutboxMessage(serialize(message)));
    }
  }

  public <T> Future<T> ask(RpcRequestMessage message, long timeoutMs, RpcResponseCallback callback) {
    RpcAddress remoteAddr = message.receiver.address();
    try {
      if (remoteAddr == address()) {
        Futures.addCallback();
      } else {
        SettableFuture<T> future = SettableFuture.create();
        OutboxMessage.RpcOutboxMessage rpcMessage = new OutboxMessage.RpcOutboxMessage(serialize(message), )
        postToOutbox(message.receiver, );

      }
    } catch (Throwable e) {

    }
  }


  private void postToOutbox(NettyRpcEndpointRef receiver, OutboxMessage message) {
    if (receiver.client() != null) {
      message.sendWith(receiver.client());
    } else {
      if (receiver.address() == null) {
        throw new RuntimeException("");
      }
      Outbox targetOutbox;
      Outbox outbox = outboxes.get(receiver.address());
      if (outbox == null) {
        Outbox newOutbox = new Outbox(this, receiver.address());
        Outbox oldOutbox = outboxes.putIfAbsent(receiver.address(), newOutbox);
        if (oldOutbox == null) {
          targetOutbox = newOutbox;
        } else {
          targetOutbox = oldOutbox;
        }
      } else {
        targetOutbox = outbox;
      }
      if (stopped.get()) {
        outboxes.remove(receiver.address());
        targetOutbox.stop();
      } else {
        targetOutbox.send(message);
      }
    }
  }

  private void cleanup() throws IOException {
    if (stopped.compareAndSet(false, true)) {
      return;
    }
    for (RpcAddress rpcAddress : outboxes.keySet()) {
      Outbox outbox = outboxes.get(rpcAddress);
      outboxes.remove(rpcAddress);
      outbox.stop();
    }
    if (dispatcher != null) {
      dispatcher.stop();
    }
    if (server != null) {
      server.close();
    }
    if (clientFactory != null) {
      clientFactory.close();
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow();
    }
  }
}
