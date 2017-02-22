package com.dts.core.rpc.network.client;

import com.dts.core.rpc.network.TransportContext;
import com.dts.core.rpc.network.server.TransportChannelHandler;
import com.dts.core.rpc.network.util.IOMode;
import com.dts.core.rpc.network.util.NettyUtils;
import com.dts.core.rpc.network.util.TransportConf;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author zhangxin
 */
public class TransportClientFactory implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;

  private final Random rand;
  private final int numConnectionPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private final Map<SocketAddress, ClientPool> connectionPool;
  private EventLoopGroup workGroup;
  private PooledByteBufAllocator pooledByteBufAllocator;

  public TransportClientFactory(TransportContext context) {
    this.context = context;
    this.conf = context.getConf();
    this.numConnectionPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    connectionPool = Maps.newConcurrentMap();
    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    this.workGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "client");
    this.pooledByteBufAllocator = NettyUtils.createPooledByteBufAllocator(conf.preferDirectBufs(),
        false, conf.clientThreads());
  }

  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    final InetSocketAddress unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort);
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);
    }
    int clientIndex = rand.nextInt(numConnectionPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];
    if (cachedClient != null && cachedClient.isActive()) {
      TransportChannelHandler handler = cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }
      if (cachedClient.isActive()) {
        logger.trace("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    final long preResolvedHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolvedHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }
    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];
      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = createClient(resolvedAddress);
      return clientPool.clients[clientIndex];
    }
  }

  private TransportClient createClient(InetSocketAddress address) throws IOException {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workGroup).channel(socketChannelClass)
        .option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
        .option(ChannelOption.ALLOCATOR, pooledByteBufAllocator);
    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        clientRef.set(clientHandler.getClient());
      }
    });

    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
      throw new IOException(
          String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }
    TransportClient client = clientRef.get();
    assert client != null : "Channel future completed successfully with null client";
    logger.info("Successfully created connection to {} after {} ms, local address: {}", address,
        (System.nanoTime() - preConnect) / 1000000, client.getChannel().localAddress());
    return client;
  }

  @Override
  public void close() {
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; ++i) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          try {
            client.close();
          } catch (IOException e) {
            logger.error("IOException should not have been thrown", e);
          }
        }
      }
    }
    connectionPool.clear();
    if (workGroup != null) {
      workGroup.shutdownGracefully();
      workGroup = null;
    }
  }

  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; ++i) {
        locks[i] = new Object();
      }
    }
  }
}
