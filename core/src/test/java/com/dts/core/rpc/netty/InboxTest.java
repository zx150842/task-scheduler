package com.dts.core.rpc.netty;

import com.dts.core.rpc.TestRpcEndpoint;
import com.dts.core.rpc.RpcAddress;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author zhangxin
 */
public class InboxTest {

  @Test
  public void testPost() {
    TestRpcEndpoint endpoint = new TestRpcEndpoint();
    NettyRpcEndpointRef endpointRef = mock(NettyRpcEndpointRef.class);
    when(endpointRef.name()).thenReturn("hello");

    Dispatcher dispatcher = mock(Dispatcher.class);

    Inbox inbox = new Inbox(endpointRef, endpoint);
    OneWayInboxMessage message = new OneWayInboxMessage(null, "hi");
    inbox.post(message);
    inbox.process(dispatcher);
    assertTrue(inbox.isEmpty());

    endpoint.verifySingleReceiveMessage("hi");

    inbox.stop();
    inbox.process(dispatcher);
    assertTrue(inbox.isEmpty());
    endpoint.verifyStarted();
    endpoint.verifyStopped();
  }

  @Test
  public void testPostWithReply() {
    TestRpcEndpoint endpoint = new TestRpcEndpoint();
    NettyRpcEndpointRef endpointRef = mock(NettyRpcEndpointRef.class);
    Dispatcher dispatcher = mock(Dispatcher.class);

    Inbox inbox = new Inbox(endpointRef, endpoint);
    RpcInboxMessage message = new RpcInboxMessage(null, "hi", null);
    inbox.post(message);
    inbox.process(dispatcher);
    assertTrue(inbox.isEmpty());

    endpoint.verifySingleReceiveAndReplyMessage("hi");
  }

  @Test
  public void testPosMultiThreads() throws InterruptedException {
    TestRpcEndpoint endpoint = new TestRpcEndpoint();
    NettyRpcEndpointRef endpointRef = mock(NettyRpcEndpointRef.class);
    when(endpointRef.name()).thenReturn("hello");

    Dispatcher dispatcher = mock(Dispatcher.class);
    AtomicInteger numDroppedMessages = new AtomicInteger(0);
    Inbox inbox = new Inbox(endpointRef, endpoint) {
      @Override
      protected void onDrop(InboxMessage message) {
        numDroppedMessages.incrementAndGet();
      }
    };

    CountDownLatch exitLatch = new CountDownLatch(10);

    for (int i = 0; i < 10; ++i) {
      new Thread() {
        @Override
        public void run() {
          for (int i = 0; i < 100; ++i) {
            OneWayInboxMessage message = new OneWayInboxMessage(null, "hi");
            inbox.post(message);
          }
          exitLatch.countDown();
        }
      }.start();
    }

    inbox.process(dispatcher);
    inbox.stop();

    inbox.process(dispatcher);
    assertTrue(inbox.isEmpty());
    exitLatch.await(30, TimeUnit.SECONDS);

    assertEquals(1000, endpoint.numReceiveMessages + numDroppedMessages.get());
    endpoint.verifyStarted();
    endpoint.verifyStopped();
  }

  @Test
  public void testPostAssociated() {
    TestRpcEndpoint endpoint = new TestRpcEndpoint();
    NettyRpcEndpointRef endpointRef = mock(NettyRpcEndpointRef.class);
    Dispatcher dispatcher = mock(Dispatcher.class);

    RpcAddress remoteAddress = new RpcAddress("localhost", 11111);

    Inbox inbox = new Inbox(endpointRef, endpoint);
    inbox.post(new RemoteProcessConnected(remoteAddress));
    inbox.process(dispatcher);

    endpoint.verifySingleOnConnectedMessage(remoteAddress);
  }

  @Test
  public void testPostDisassociated() {
    TestRpcEndpoint endpoint = new TestRpcEndpoint();
    NettyRpcEndpointRef endpointRef = mock(NettyRpcEndpointRef.class);
    Dispatcher dispatcher = mock(Dispatcher.class);

    RpcAddress remoteAddress = new RpcAddress("localhost", 11111);

    Inbox inbox = new Inbox(endpointRef, endpoint);
    inbox.post(new RemoteProcessDisconnected(remoteAddress));
    inbox.process(dispatcher);

    endpoint.verifySingleOnDisconnectedMessage(remoteAddress);
  }

  @Test
  public void testPostAssociationError() {
    TestRpcEndpoint endpoint = new TestRpcEndpoint();
    NettyRpcEndpointRef endpointRef = mock(NettyRpcEndpointRef.class);
    Dispatcher dispatcher = mock(Dispatcher.class);

    RpcAddress remoteAddress = new RpcAddress("localhost", 11111);
    Exception cause = new RuntimeException("Oops");

    Inbox inbox = new Inbox(endpointRef, endpoint);
    inbox.post(new RemoteProcessConnectionError(cause, remoteAddress));
    inbox.process(dispatcher);

    endpoint.verifySingleOnNetworkErrorMessage(cause, remoteAddress);
  }
}
