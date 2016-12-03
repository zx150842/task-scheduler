package com.dts.rpc.network.util;

import java.util.concurrent.*;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.sctp.nio.NioSctpChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Created by zhangxin on 2016/11/26.
 */
public class NettyUtils {

  public static String getRemoteAddress(Channel channel) {
    if (channel != null && channel.remoteAddress() != null) {
      return channel.remoteAddress().toString();
    }
    return "unknown remote";
  }

  public static PooledByteBufAllocator createPooledByteBufAllocator(boolean allowDirectBufs,
      boolean allowCache, int numCores) {
    return new PooledByteBufAllocator();
  }

  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = createThreadFactory(threadPrefix);
    switch (mode) {
      case NIO:
        return new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("");
    }
  }

  public static ThreadFactory createThreadFactory(String threadPrefix) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadPrefix + "-%d").build();
  }

  public static ThreadPoolExecutor newDaemonCachedThreadPool(String prefix, int maxThreadNum, int keepAliveSec) {
    ThreadFactory threadFactory = createThreadFactory(prefix);
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
      maxThreadNum,
      maxThreadNum,
      keepAliveSec,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(),
      threadFactory
    );
    threadPool.allowCoreThreadTimeOut(true);
    return threadPool;
  }

  public static Class<? extends ServerChannel> getServerChannel(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioServerSocketChannel.class;
      case EPOLL:
        return EpollServerSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }
}
