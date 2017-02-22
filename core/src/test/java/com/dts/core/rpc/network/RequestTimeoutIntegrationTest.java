package com.dts.core.rpc.network;

import com.dts.core.util.AddressUtil;
import com.google.common.collect.Maps;

import com.dts.core.rpc.network.util.MapConfigProvider;
import com.dts.core.rpc.network.client.RpcResponseCallback;
import com.dts.core.rpc.network.client.TransportClient;
import com.dts.core.rpc.network.client.TransportClientFactory;
import com.dts.core.rpc.network.server.RpcHandler;
import com.dts.core.rpc.network.server.TransportServer;
import com.dts.core.rpc.network.util.TransportConf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author zhangxin
 */
public class RequestTimeoutIntegrationTest {
  private TransportServer server;
  private TransportClientFactory clientFactory;
  private TransportConf conf;

  private static final int FOREVER = 60 * 1000;

  @Before
  public void setUp() throws Exception {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put("dts.rpc.io.connectionTimeout", "10000");
    conf = new TransportConf("rpc", new MapConfigProvider(configMap));
  }

  @After
  public void tearDown() throws IOException {
    if (server != null) {
      server.close();
    }
    if (clientFactory != null) {
      clientFactory.close();
    }
  }

  @Test
  public void timeoutInactiveRequests() throws Exception {
    final Semaphore semaphore = new Semaphore(1);
    final int responseSize = 16;
    RpcHandler handler = new RpcHandler() {
      @Override public void receive(TransportClient client, ByteBuffer message,
        RpcResponseCallback callback) {
        try {
          semaphore.acquire();
          callback.onSuccess(ByteBuffer.allocate(responseSize));
        } catch (InterruptedException e) {
          // do nothing
        }
      }
    };
    TransportContext context = new TransportContext(conf, handler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
    TransportClient client = clientFactory.createClient(AddressUtil.getLocalHost(), server.getPort());

    TestCallback callback0 = new TestCallback();
    client.sendRpc(ByteBuffer.allocate(0), callback0);
    callback0.latch.await();
    assertEquals(responseSize, callback0.successLength);

    TestCallback callback1 = new TestCallback();
    client.sendRpc(ByteBuffer.allocate(0), callback1);
    callback1.latch.await(60, TimeUnit.SECONDS);
    assertNotNull(callback1.failure);
    assertTrue(callback1.failure instanceof IOException);

    semaphore.release();
  }

  @Test
  public void timeoutCleanlyClosesClient() throws Exception {
    // TODO cache client
  }

  private static class TestCallback implements RpcResponseCallback {
    int successLength = -1;
    Throwable failure;
    final CountDownLatch latch = new CountDownLatch(1);

    @Override public void onSuccess(ByteBuffer response) {
      successLength = response.remaining();
      latch.countDown();
    }

    @Override public void onFailure(Throwable e) {
      failure = e;
      latch.countDown();
    }
  }
}
