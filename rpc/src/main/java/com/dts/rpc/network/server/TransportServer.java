package com.dts.rpc.network.server;

import com.dts.rpc.network.TransportContext;
import com.dts.rpc.network.util.IOMode;
import com.dts.rpc.network.util.NettyUtils;
import com.dts.rpc.network.util.TransportConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

/**
 * @author zhangxin
 */
public class TransportServer implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportServer.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final RpcHandler appRpcHandler;

  private ServerBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private int port = -1;

  public TransportServer(TransportContext context, String host, int port,
      RpcHandler appRpcHandler) {
    this.context = context;
    this.conf = context.getConf();
    this.appRpcHandler = appRpcHandler;

    try {
      init(host, port);
    } catch (RuntimeException e) {
      try {
        close();
      } catch (IOException e1) {
        logger.error("IOException should not have been thrown.", e1);
      }
      throw e;
    }
  }

  public int getPort() {
    if (port == -1) {
      throw new IllegalStateException("Server not initialized");
    }
    return port;
  }

  private void init(String host, int port) {
    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    EventLoopGroup bossGroup = NettyUtils.createEventLoop(ioMode, conf.serverThreads(), "server");
    EventLoopGroup workerGroup = bossGroup;
    PooledByteBufAllocator allocator = NettyUtils
        .createPooledByteBufAllocator(conf.preferDirectBufs(), true, conf.serverThreads());
    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NettyUtils.getServerChannel(ioMode))
      .option(ChannelOption.ALLOCATOR, allocator)
      .childOption(ChannelOption.ALLOCATOR, allocator);

    if (conf.backLog() > 0) {
      bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
    }
    if (conf.receiveBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }
    if (conf.sendBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        RpcHandler rpcHandler = appRpcHandler;
        context.initializePipeline(ch, rpcHandler);
      }
    });

    InetSocketAddress address =
        host == null ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
    channelFuture = bootstrap.bind(address);
    channelFuture.syncUninterruptibly();
    this.port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    logger.debug("Server started on port: " + port);
  }

  @Override
  public void close() throws IOException {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      channelFuture = null;
    }
    if (bootstrap != null && bootstrap.config().group() != null) {
      bootstrap.config().group().shutdownGracefully();
    }
    if (bootstrap != null && bootstrap.config().childGroup() != null) {
      bootstrap.config().childGroup().shutdownGracefully();
    }
    bootstrap = null;
  }
}
