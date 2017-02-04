package com.dts.core.rpc.netty;

import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.netty.Dispatcher;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.rpc.netty.NettyRpcHandler;
import com.dts.core.rpc.netty.RpcRequestMessage;
import com.dts.core.rpc.network.client.TransportClient;
import com.dts.core.rpc.network.client.TransportResponseHandler;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import io.netty.channel.Channel;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author zhangxin
 */
public class NettyRpcHandlerTest {

  private NettyRpcEnv env;

  @Before
  public void setUp() {
    env = mock(NettyRpcEnv.class);
    when(env.deserialize(any(TransportClient.class), any(ByteBuffer.class)))
    .thenReturn(new RpcRequestMessage(new RpcAddress("localhost", 12345), null, null));
  }

  @Test
  public void testReceive() {
    Dispatcher dispatcher = mock(Dispatcher.class);
    NettyRpcHandler nettyRpcHandler = new NettyRpcHandler(dispatcher, env);

    Channel channel = mock(Channel.class);
    TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
    InetSocketAddress address = new InetSocketAddress("localhost", 40000);
    when(channel.remoteAddress()).thenReturn(address);
    nettyRpcHandler.channelActive(client);

    ArgumentCaptor<RemoteProcessConnected> argumentCaptor = ArgumentCaptor.forClass(RemoteProcessConnected.class);

    verify(dispatcher, times(1)).postToAll(argThat(new ArgumentMatcher<RemoteProcessConnected>() {
      @Override public boolean matches(RemoteProcessConnected msg) {
        return msg.remoteAddress.host.equals(address.getHostString())
          && msg.remoteAddress.port == address.getPort();
      }
    }));
  }

  @Test
  public void testConnectionTerminated() {
    Dispatcher dispatcher = mock(Dispatcher.class);
    NettyRpcHandler nettyRpcHandler = new NettyRpcHandler(dispatcher, env);

    Channel channel = mock(Channel.class);
    TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
    InetSocketAddress address = new InetSocketAddress("localhost", 40000);
    when(channel.remoteAddress()).thenReturn(address);
    nettyRpcHandler.channelActive(client);

    when(channel.remoteAddress()).thenReturn(address);
    nettyRpcHandler.channelInactive(client);

    verify(dispatcher, times(1)).postToAll(argThat(new ArgumentMatcher<RemoteProcessConnected>() {
      @Override public boolean matches(RemoteProcessConnected msg) {
        return msg.remoteAddress.host.equals(address.getHostString())
          && msg.remoteAddress.port == address.getPort();
      }
    }));
    verify(dispatcher, times(1)).postToAll(argThat(new ArgumentMatcher<RemoteProcessDisconnected>() {
      @Override public boolean matches(RemoteProcessDisconnected msg) {
        return msg.remoteAddress.host.equals(address.getHostString())
          && msg.remoteAddress.port == address.getPort();
      }
    }));
  }
}
