package com.dts.rpc.netty;

import com.dts.rpc.*;
import com.dts.rpc.exception.DTSException;
import com.dts.rpc.network.TransportContext;
import com.dts.rpc.network.client.TransportClient;
import com.dts.rpc.network.client.TransportClientFactory;
import com.dts.rpc.network.server.TransportServer;
import com.dts.rpc.network.util.NettyUtils;
import com.dts.rpc.network.util.TransportConf;
import com.dts.rpc.util.SerializerInstance;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhangxin
 */
public class NettyRpcEnv extends RpcEnv {
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

  private static ThreadLocal<NettyRpcEnv> nettyRpcEnvTL = new InheritableThreadLocal<>();
  private static ThreadLocal<TransportClient> clientTL = new InheritableThreadLocal<>();

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

  public static NettyRpcEnv currentEnv() {
    return nettyRpcEnvTL.get();
  }

  public static TransportClient currentClient() {
    return clientTL.get();
  }

  public void startServer(int port) {
    server = transportContext.createServer(host, port);
    dispatcher.registerRpcEndpoint(RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher));
    nettyRpcEnvTL.set(this);
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
    TransportClient client = clientFactory.createClient(new InetSocketAddress(address.host, address.port));
    clientTL.set(client);
    return client;
  }

  public void removeOutbox(RpcAddress address) {
    Outbox outbox = outboxes.remove(address);
    if (outbox != null) {
      outbox.stop();
    }
  }

  public RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint) {
    return dispatcher.registerRpcEndpoint(name, endpoint);
  }

  public RpcEndpointRef setupEndpointRef(RpcAddress address, String endpointName) {
    NettyRpcEndpointRef verifier = new NettyRpcEndpointRef(conf, new RpcEndpointAddress(address, RpcEndpointVerifier.NAME),this);
    Future future = verifier.ask(new RpcEndpointVerifier.CheckExistence(endpointName));
    try {
      // TODO add timeout
      boolean verified = (boolean)future.get(100000, TimeUnit.SECONDS);
      RpcEndpointAddress rpcEndpointAddress = new RpcEndpointAddress(address, endpointName);
      if (verified) {
        return new NettyRpcEndpointRef(conf, rpcEndpointAddress, this);
      }
      throw new DTSException("Cannot find endpoint: " + rpcEndpointAddress);
    } catch (Throwable e) {
      throw new DTSException(e);
    }
  }

  public ByteBuffer serialize(Object content) {
    return serializerInstance.serialize(content);
  }

  public Object deserialize(TransportClient client, ByteBuffer bytes) {
    return serializerInstance.deserialize(bytes);
  }

  public RpcEndpointRef endpointRef(RpcEndpoint endpoint) {
    return dispatcher.getRpcEndpointRef(endpoint);
  }

  public void stop(RpcEndpointRef endpointRef) {
    assert endpointRef instanceof NettyRpcEndpointRef;
    dispatcher.stop(endpointRef);
  }

  public void shutdown() {
    try {
      cleanup();
    } catch (IOException e) {
      throw new DTSException(e);
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
      postToOutbox(message.receiver, new OneWayOutboxMessage(serialize(message)));
    }
  }

  public <T> Future<T> ask(RpcRequestMessage message) {
    SettableFuture<T> future = SettableFuture.create();
    RpcAddress remoteAddr = message.receiver.address();
    try {
      if (remoteAddr == address()) {
        dispatcher.postLocalMessage(message, future);
      } else {
        RpcOutboxMessage rpcMessage = new RpcOutboxMessage(serializerInstance, serialize(message), future);
        postToOutbox(message.receiver, rpcMessage);
      }
    } catch (Throwable e) {
      logger.warn("Ignored failure", e);
    }
    return future;
  }

  private void postToOutbox(NettyRpcEndpointRef receiver, OutboxMessage message) {
    if (receiver.client() != null) {
      message.sendWith(receiver.client());
    } else {
      if (receiver.address() == null) {
        throw new DTSException("Cannot send message to client endpoint with no listen address");
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
    if (!stopped.compareAndSet(false, true)) {
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
