package com.dts.rpc.network.client;

import com.dts.rpc.network.TransportContext;
import com.dts.rpc.network.server.TransportChannelHandler;
import com.dts.rpc.network.util.IOMode;
import com.dts.rpc.network.util.NettyUtils;
import com.dts.rpc.network.util.TransportConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Created by zhangxin on 2016/11/26.
 */
public class TransportClientFactory implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;

  private final Random rand;
  private final int numConnectionPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workGroup;
  private PooledByteBufAllocator pooledByteBufAllocator;

  public TransportClientFactory(TransportContext context) {
    this.context = context;
    this.conf = context.getConf();
    this.numConnectionPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    this.workGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "client");
    this.pooledByteBufAllocator = NettyUtils.createPooledByteBufAllocator(conf.preferDirectBufs(),
        false, conf.clientThreads());
  }

  public TransportClient createClient(InetSocketAddress address) throws IOException {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workGroup).channel(NioSocketChannel.class)
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
    logger.info("Successfully created connection to {} after {} ms", address,
        (System.nanoTime() - preConnect) / 1000000);
    return client;
  }

  @Override
  public void close() throws IOException {

    if (workGroup != null) {
      workGroup.shutdownGracefully();
      workGroup = null;
    }
  }
}
