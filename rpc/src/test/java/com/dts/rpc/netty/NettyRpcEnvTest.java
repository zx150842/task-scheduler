package com.dts.rpc.netty;

import com.dts.rpc.*;
import com.dts.rpc.network.TestUtils;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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
    return new NettyRpcEnvFactory().create(conf, TestUtils.getLocalHost(), port, clientMode);
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
    assertTrue(cause.getCause() instanceof RuntimeException);
    assertTrue(cause.getCause().getMessage().contains(uri));
  }

  @Test
  public void sendMsgToLocal() throws InterruptedException {
    List<String> messages = Lists.newArrayList();
    RpcEndpointRef rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        messages.add((String)o);
      }

      @Override public void receiveAndReply(Object o, RpcCallContext context) {

      }
    });
    rpcEndpointRef.send("hello");
    awaitForComplete(messages);
    assertEquals(1, messages.size());
    assertEquals("hello", messages.get(0));
  }

  @Test
  public void sendMsgToRemote() {
    List<String> messages = Lists.newArrayList();
    env.setupEndpoint("send-remotely", new RpcEndpoint(env) {
      @Override public void receive(Object o) {
        messages.add((String)o);
      }

      @Override public void receiveAndReply(Object o, RpcCallContext context) {

      }
    });

    RpcEnv anotherEnv = createRpcEnv(new DTSConf(false), "remote", 0, true);
    RpcEndpointRef rpcEndpointRef = anotherEnv.setupEndpointRef(env.address(), "send-remotely");
    try {
      rpcEndpointRef.send("hello");
      awaitForComplete(messages);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      anotherEnv.shutdown();
      anotherEnv.awaitTermination();
    }
  }

  private void awaitForComplete(List list) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (System.nanoTime() < deadline && list.size() == 0) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }
}
