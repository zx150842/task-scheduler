package com.dts.core.rpc.network.server;

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author zhangxin
 */
public class EchoServer {
  private final int port;

  public EchoServer(int port) {
    this.port = port;
  }

  public static void main(String[] args) throws InterruptedException {
    int port = 10000;
    new EchoServer(port).start();
  }

  public void start() throws InterruptedException {
    final EchoServerHandler serverHandler = new EchoServerHandler();
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(group).channel(NioServerSocketChannel.class).localAddress(new InetSocketAddress(port))
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ch.pipeline().addLast(serverHandler);
            }
          });
      ChannelFuture f = b.bind().sync();
      f.channel().closeFuture().sync();
    } finally {
      group.shutdownGracefully().sync();
    }
  }
}
