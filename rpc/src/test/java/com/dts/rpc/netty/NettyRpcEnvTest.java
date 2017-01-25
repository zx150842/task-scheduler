package com.dts.rpc.netty;

import com.dts.rpc.*;
import com.dts.rpc.exception.DTSException;
import com.dts.rpc.exception.DTSSerializeException;
import com.dts.rpc.network.TestUtils;
import com.dts.rpc.util.TestPair;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author zhangxin
 */
public class NettyRpcEnvTest {

  private RpcEnv env;

  @Before
  public void setUp() {
    DTSConf conf = new DTSConf(false);
    env = createRpcEnv(conf, "local", 0, false);
  }

  @After
  public void tearDown() {
    if (env != null) {
      env.shutdown();
    }
  }

  protected RpcEnv createRpcEnv(DTSConf conf, String name, int port, boolean clientMode) {
    return RpcEnv.create(name, TestUtils.getLocalHost(), port, conf, clientMode);
  }

  @Test
  public void testNonExistEndpoint() {
    String uri = new RpcEndpointAddress(env.address(), "nonexist-endpoint").toString();
    Throwable cause = new Throwable();
    try {
      env.setupEndpointRef(env.address(), "nonexist-endpoint");
    } catch (Exception e) {
      cause = e;
    }
    assertTrue(cause.getCause() instanceof DTSException);
    assertTrue(cause.getCause().getMessage().contains(uri));
  }

  @Test
  public void testSendMsgToLocal() throws InterruptedException {
    List<String> messages = Lists.newArrayList();
    RpcEndpointRef rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        messages.add((String)o);
      }
    });
    rpcEndpointRef.send("hello");
    awaitForComplete(messages);
    assertEquals(1, messages.size());
    assertEquals("hello", messages.get(0));
  }

  @Test
  public void testSendMsgToRemote() {
    List<String> messages = Lists.newArrayList();
    env.setupEndpoint("send-remotely", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        messages.add((String)o);
      }
    });
    DTSConf conf = new DTSConf(false);
    RpcEnv anotherEnv = createRpcEnv(conf, "remote", 0, true);
    RpcEndpointRef rpcEndpointRef = anotherEnv.setupEndpointRef(env.address(), "send-remotely");
    try {
      rpcEndpointRef.send("hello");
      awaitForComplete(messages);
      assertEquals("hello", messages.get(0));
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  @Test
  public void testSendRpcEndpointRef() {
    RpcEndpoint endpoint = new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        if ("Hello".equals(o)) {
          context.reply(self());
        } else if ("Echo".equals(o)) {
          context.reply("Echo");
        }
      }
    };
    RpcEndpointRef rpcEndpointRef = env.setupEndpoint("send-ref", endpoint);
    RpcEndpointRef newRpcEndpointRef = rpcEndpointRef.askWithRetry("Hello");
    String reply = newRpcEndpointRef.askWithRetry("Echo");
    assertEquals("Echo", reply);
  }

  @Test
  public void testAskMsgToLocal() {
    RpcEndpointRef endpointRef = env.setupEndpoint("sendRpc-locally", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        if (o instanceof String) {
          context.reply(o);
        }
      }
    });
    String reply = endpointRef.askWithRetry("hello");
    assertEquals("hello", reply);
  }

  @Test
  public void testAskMsgToRemote() {
    env.setupEndpoint("sendRpc-remotely", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        if (o instanceof String) {
          context.reply(o);
        }
      }
    });
    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", 0, true);
    RpcEndpointRef endpointRef = anotherEnv.setupEndpointRef(env.address(), "sendRpc-remotely");
    try {
      String reply = endpointRef.askWithRetry("hello");
      assertEquals("hello", reply);
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  @Test
  public void testAskMsgTimeout() {
    env.setupEndpoint("ask-timeout", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        if (o instanceof String) {
          try {
            Thread.sleep(10);
            context.reply(o);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    DTSConf conf = new DTSConf(false);
    conf.set("dts.rpc.retry.waitTimeMs", "1");
    conf.set("dts.rpc.numRetries", "1");
    RpcEnv anotherEnv = createRpcEnv(conf, "remote", 0, true);
    RpcEndpointRef rpcEndpointRef = anotherEnv.setupEndpointRef(env.address(), "ask-timeout");
    try {
      Throwable cause = new Throwable();
      try {
        rpcEndpointRef.askWithRetry("hello", 1);
      } catch (RuntimeException e) {
        cause = e;
      }
      assertTrue(cause.getCause() instanceof TimeoutException);
      assertTrue(cause.getMessage().contains("Error sending message"));
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  @Test
  public void testOnStartOnStop() throws InterruptedException {
    CountDownLatch stopLatch = new CountDownLatch(1);
    List<String> calledMethods = Lists.newArrayList();

    RpcEndpoint endpoint = new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        // do nothing
      }

      @Override public void onStart() {
        calledMethods.add("start");
      }

      @Override public void onStop() {
        calledMethods.add("stop");
        stopLatch.countDown();
      }
    };
    RpcEndpointRef rpcEndpointRef = env.setupEndpoint("start-stop-test", endpoint);
    env.stop(rpcEndpointRef);
    stopLatch.await(10, TimeUnit.SECONDS);
    assertEquals(Lists.newArrayList("start", "stop"), calledMethods);
  }

  @Test
  public void testErrorOnStart() throws InterruptedException {
    final List<Throwable> e = Lists.newArrayList();
    env.setupEndpoint("onError-onStart", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        // do nothing
      }

      @Override public void onError(Throwable cause) throws Throwable {
        e.add(cause);
      }

      @Override public void onStart() {
        throw new RuntimeException("Oops!");
      }
    });
    awaitForComplete(e);
    assertEquals("Oops!", e.get(0).getMessage());
  }

  @Test
  public void testErrorOnStop() throws InterruptedException {
    List<Throwable> e = Lists.newArrayList();
    RpcEndpointRef endpointRef = env.setupEndpoint("onError-onStop", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        // do nothing
      }

      @Override public void onError(Throwable cause) throws Throwable {
        e.add(cause);
      }

      @Override public void onStop() {
        throw new RuntimeException("Oops!");
      }
    });
    env.stop(endpointRef);
    awaitForComplete(e);
    assertEquals("Oops!", e.get(0).getMessage());
  }

  @Test
  public void testErrorInReceive() throws InterruptedException {
    List<Throwable> e = Lists.newArrayList();
    RpcEndpointRef endpointRef = env.setupEndpoint("onError-receive", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        throw new RuntimeException("Oops!");
      }

      @Override public void onError(Throwable cause) throws Throwable {
        e.add(cause);
      }
    });
    endpointRef.send("Foo");
    awaitForComplete(e);
    assertEquals("Oops!", e.get(0).getMessage());
  }

  @Test
  public void testSelfCallOnStart() throws InterruptedException {
    final AtomicBoolean callSelfSucc = new AtomicBoolean(false);
    env.setupEndpoint("self-onStart", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        // do nothing
      }

      @Override public void onStart() {
        self();
        callSelfSucc.set(true);
      }
    });

    awaitForComplete(Lists.newArrayList(callSelfSucc));
    assertTrue(callSelfSucc.get());
  }

  @Test
  public void testSelfCallInReceive() throws InterruptedException {
    AtomicBoolean callSelfSucc = new AtomicBoolean(false);
    RpcEndpointRef endpointRef = env.setupEndpoint("self-receive", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        self();
        callSelfSucc.set(true);
      }
    });
    endpointRef.send("Foo");
    awaitForComplete(Lists.newArrayList(callSelfSucc));
    assertTrue(callSelfSucc.get());
  }

  @Test
  public void testSelfCallOnStop() throws InterruptedException {
    List<RpcEndpointRef> self = Lists.newArrayList();

    RpcEndpointRef endpointRef = env.setupEndpoint("self-onStop", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        // do nothing
      }

      @Override public void onStop() {
        self.add(self());
      }
    });
    env.stop(endpointRef);
    awaitForComplete(self);
    assertEquals(null, self.get(0));
  }

  @Test
  public void testCallReceiveInSequence() throws InterruptedException {
    for (int i = 0; i < 100; ++i) {
      AtomicInteger result = new AtomicInteger(0);
      RpcEndpointRef endpointRef = env.setupEndpoint("receive-in-sequence-" + i, new RpcEndpoint(env) {
        @Override public void receive(Object o) {
          result.incrementAndGet();
        }
      });
      for (int j = 0; j < 10; ++j) {
        new Thread() {
          @Override
          public void run() {
            for (int k = 0; k < 100; ++k) {
              endpointRef.send("Hello");
            }
          }
        }.start();
      }
      TimeUnit.MILLISECONDS.sleep(100);
      assertEquals(1000, result.get());
      env.stop(endpointRef);
    }
  }

  @Test
  public void testStopRpcEndpointRef() throws InterruptedException {
    AtomicInteger onStopCount = new AtomicInteger(0);
    RpcEndpointRef endpointRef = env.setupEndpoint("stop-reentrant", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        // do nothing
      }

      @Override public void onStop() {
        onStopCount.incrementAndGet();
      }
    });

    env.stop(endpointRef);
    env.stop(endpointRef);
    TimeUnit.SECONDS.sleep(2);
    assertEquals(1, onStopCount.get());
  }

  @Test
  public void testSendWithReplyLocal() throws Exception {
    RpcEndpointRef endpointRef = env.setupEndpoint("sendWithReply", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        context.reply("ack");
      }
    });
    Future<String> future = endpointRef.ask("Hi");
    String result = future.get(2, TimeUnit.SECONDS);
    assertEquals("ack", result);
    env.stop(endpointRef);
  }

  @Test
  public void testSendWithReplyRemote()
    throws InterruptedException, ExecutionException, TimeoutException {
    env.setupEndpoint("sendWithReply-remotely", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        context.reply("ack");
      }
    });

    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", 0, true);
    RpcEndpointRef endpointRef = anotherEnv.setupEndpointRef(env.address(), "sendWithReply-remotely");
    try {
      Future<String> future = endpointRef.ask("hello");
      String ack = future.get(2, TimeUnit.SECONDS);
      assertEquals("ack", ack);
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  @Test
  public void testSendWithReplyLocalError()
    throws InterruptedException, ExecutionException, TimeoutException {
    RpcEndpointRef endpointRef = env.setupEndpoint("sendWithReply-error", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        context.sendFailure(new DTSException("Oops"));
      }
    });
    Future<String> future = endpointRef.ask("Hi");
    Throwable cause = new Throwable();
    try {
      future.get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      cause = e;
    }
    assertEquals("Oops", cause.getCause().getMessage());
    env.stop(endpointRef);
  }

  @Test
  public void testSendWithReplyRemoteError()
    throws InterruptedException, ExecutionException, TimeoutException {
    env.setupEndpoint("sendWithReply-remotely-error", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        context.sendFailure(new DTSException("Oops"));
      }
    });

    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", 0, true);
    RpcEndpointRef endpointRef = anotherEnv.setupEndpointRef(env.address(), "sendWithReply-remotely-error");
    try {
      Future<String> future = endpointRef.ask("hello");
      Throwable cause = new Throwable();
      try {
        String result = future.get(200, TimeUnit.SECONDS);
        System.out.println(result);
      } catch (Exception e) {
        cause = e;
      }
      assertEquals("Oops", cause.getCause().getMessage());
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  private TestPair setupNetworkEndpoint(RpcEnv _env) {
    ConcurrentLinkedQueue<TestPair> events = Queues.newConcurrentLinkedQueue();
    RpcEndpointRef endpointRef = _env.setupEndpoint("network-events-non-client", new RpcEndpoint(_env) {
      @Override public void receive(Object o) {
        if (!"hello".equals(o)) {
          events.add(new TestPair("receive", o));
        }
      }

      @Override public void onConnected(RpcAddress remoteAddress) {
        events.add(new TestPair("onConnected", remoteAddress));
      }

      @Override public void onDisconnected(RpcAddress remoteAddress) {
        events.add(new TestPair("onDisconnected", remoteAddress));
      }

      @Override public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
        events.add(new TestPair("onNetworkError", remoteAddress));
      }
    });
    return new TestPair<>(endpointRef, events);
  }

  @Test
  public void testNetworkEventsInServerEnvWhenAnotherEnvInServerMode() throws InterruptedException {
    RpcEnv serverEnv1 = createRpcEnv(new DTSConf(false), "server1", 0, false);
    RpcEnv serverEnv2 = createRpcEnv(new DTSConf(false), "server2", 0, false);
    TestPair pair1 = setupNetworkEndpoint(serverEnv1);
    ConcurrentLinkedQueue events = (ConcurrentLinkedQueue) pair1.getRight();
    TestPair pair2 = setupNetworkEndpoint(serverEnv2);
    RpcEndpointRef serverRef2 = (RpcEndpointRef) pair2.getLeft();
    try {
      RpcEndpointRef serverRefInServer2 = serverEnv1.setupEndpointRef(serverEnv2.address(), serverRef2.name());
      serverRefInServer2.send("hello");
      TimeUnit.SECONDS.sleep(2);
      assertTrue(events.contains(new TestPair<>("onConnected", serverEnv2.address())));

      serverEnv2.shutdown();
      serverEnv2.awaitTermination();

      TimeUnit.SECONDS.sleep(2);
      assertTrue(events.contains(new TestPair<>("onConnected", serverEnv2.address())));
      assertTrue(events.contains(new TestPair<>("onDisconnected", serverEnv2.address())));
    } finally {
      serverEnv1.shutdown();
      serverEnv2.shutdown();
      serverEnv1.awaitTermination();
      serverEnv2.awaitTermination();
    }
  }

  @Test
  public void testNetworkEventInServerEnvWhenAnotherEnvInClientMode() throws InterruptedException {
    RpcEnv serverEnv = createRpcEnv(new DTSConf(false), "server", 0, false);
    TestPair pair = setupNetworkEndpoint(serverEnv);
    RpcEndpointRef serverRef = (RpcEndpointRef) pair.getLeft();
    ConcurrentLinkedQueue<TestPair> events = (ConcurrentLinkedQueue) pair.getRight();
    RpcEnv clientEnv = createRpcEnv(new DTSConf(false), "client", 0, true);
    try {
      RpcEndpointRef serverRefInClient = clientEnv.setupEndpointRef(serverRef.address(), serverRef.name());
      serverRefInClient.send("hello");
      TimeUnit.SECONDS.sleep(2);
      String onConnected = null;
      for (TestPair event : events) {
        if (event.getLeft().equals("onConnected")) {
          onConnected = "onConnected";
          break;
        }
      }
      assertEquals("onConnected", onConnected);

      clientEnv.shutdown();
      clientEnv.awaitTermination();

      TimeUnit.SECONDS.sleep(2);
      onConnected = null;
      String onDisconnected = null;
      for (TestPair event : events) {
        if (event.getLeft().equals("onConnected")) {
          onConnected = "onConnected";
        } else if (event.getLeft().equals("onDisconnected")) {
          onDisconnected = "onDisconnected";
        }
      }
      assertEquals("onConnected", onConnected);
      assertEquals("onDisconnected", onDisconnected);
    } finally {
      clientEnv.shutdown();
      serverEnv.shutdown();
      clientEnv.awaitTermination();
      serverEnv.awaitTermination();
    }
  }

  @Test
  public void testNetworkEventInClientEnvWhenAnotherEnvInServerMode() throws InterruptedException {
    RpcEnv clientEnv = createRpcEnv(new DTSConf(false), "client", 0, true);
    RpcEnv serverEnv = createRpcEnv(new DTSConf(false), "server", 0, false);
    TestPair pair1 = setupNetworkEndpoint(clientEnv);
    ConcurrentLinkedQueue<TestPair> events = (ConcurrentLinkedQueue<TestPair>) pair1.getRight();
    TestPair pair2 = setupNetworkEndpoint(serverEnv);
    RpcEndpointRef serverRef = (RpcEndpointRef) pair2.getLeft();
    try {
      RpcEndpointRef serverRefInClient = clientEnv.setupEndpointRef(serverRef.address(), serverRef.name());
      serverRefInClient.send("hello");
      TimeUnit.SECONDS.sleep(2);
      assertTrue(events.contains(new TestPair("onConnected", serverEnv.address())));

      serverEnv.shutdown();
      serverEnv.awaitTermination();

      TimeUnit.SECONDS.sleep(2);
      assertTrue(events.contains(new TestPair("onConnected", serverEnv.address())));
      assertTrue(events.contains(new TestPair("onDisconnected", serverEnv.address())));
    } finally {
      clientEnv.shutdown();
      serverEnv.shutdown();
      clientEnv.awaitTermination();
      serverEnv.awaitTermination();
    }
  }

  @Test
  public void testSendWithReplyUnSerializableError() throws InterruptedException {
    env.setupEndpoint("sendWithReply-unserializable-error", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) {
        context.sendFailure(new UnserializableException());
      }
    });
    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", 0, true);
    RpcEndpointRef endpointRef = anotherEnv.setupEndpointRef(env.address(), "sendWithReply-unserializable-error");
    try {
      Throwable cause = new Throwable();
      Future<String> future = endpointRef.ask("hello");
      try {
        future.get(100, TimeUnit.SECONDS);
      } catch (Exception e) {
        cause = e;
      }
      assertTrue(cause.getCause() instanceof DTSSerializeException);
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  @Test
  public void testPortConflict() {
    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", env.address().port, false);
    assertTrue(anotherEnv.address().port != env.address().port);
  }

  @Test
  public void testAskMsgTimeoutOnFuture() throws Exception {
    RpcEndpointRef endpointRef = env.setupEndpoint("ask-future", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) throws Exception {
        if (o instanceof String) {
          context.reply(o);
        }
      }
    });
    Future<String> future1 = endpointRef.ask("hello");
    String reply1 = future1.get(1, TimeUnit.SECONDS);
    assertEquals("hello", reply1);

    Future<String> future2 = endpointRef.ask(new NeverReply("doh"));
    Throwable cause = new Throwable();
    try {
      String reply2 = future2.get(10, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      cause = e;
    }
    assertTrue(cause instanceof TimeoutException);
  }

  @Test
  public void testEnvShutdown() throws Exception {
    env.setupEndpoint("rpcEnv-shutdown", new RpcEndpoint(env) {
      @Override public void receiveAndReply(Object o, RpcCallContext context) throws Exception {
        context.reply(o);
      }
    });
    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", 0, false);
    RpcEndpoint endpoint = mock(RpcEndpoint.class);
    anotherEnv.setupEndpoint("rpcEnv-shutdown", endpoint);
    RpcEndpointRef endpointRef = anotherEnv.setupEndpointRef(env.address(), "rpcEnv-shutdown");
    String result = endpointRef.askWithRetry("hello");
    assertEquals("hello", result);
    anotherEnv.shutdown();
    anotherEnv.awaitTermination();
    env.stop(endpointRef);

    verify(endpoint).onStart();
    verify(endpoint, never()).onDisconnected(any());
    verify(endpoint, never()).onNetworkError(any(), any());
  }

  private void awaitForComplete(List list) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (System.nanoTime() < deadline && list.size() == 0) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }

  class NeverReply {
    public final String msg;
    public NeverReply(String msg) {
      this.msg = msg;
    }
  }


  class UnserializableClass {}

  class UnserializableException extends Exception {
    private UnserializableClass unserializableField = new UnserializableClass();
  }
}
