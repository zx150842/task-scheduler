package com.dts.core.rpc.network;

import com.dts.core.rpc.network.util.ConfigProvider;
import com.dts.core.util.AddressUtil;
import com.google.common.collect.Maps;

import com.dts.core.rpc.network.util.MapConfigProvider;
import com.dts.core.rpc.network.util.SystemPropertyConfigProvider;
import com.dts.core.rpc.network.client.TransportClient;
import com.dts.core.rpc.network.client.TransportClientFactory;
import com.dts.core.rpc.network.server.NoOpRpcHandler;
import com.dts.core.rpc.network.server.RpcHandler;
import com.dts.core.rpc.network.server.TransportServer;
import com.dts.core.rpc.network.util.TransportConf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author zhangxin
 */
public class TransportClientFactoryTest {
  private TransportConf conf;
  private TransportContext context;
  private TransportServer server1;
  private TransportServer server2;

  @Before
  public void setUp() {
    conf = new TransportConf("rpc", new SystemPropertyConfigProvider());
    RpcHandler rpcHandler = new NoOpRpcHandler();
    context = new TransportContext(conf, rpcHandler);
    server1 = context.createServer(AddressUtil.getLocalHost(), 8000);
    server2 = context.createServer(AddressUtil.getLocalHost(), 8001);
  }

  @After
  public void tearDown() {
    try {
      server1.close();
      server2.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testClientReuse(final int maxConnections, boolean concurrent)
    throws InterruptedException, IOException {
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put("dts.rpc.numConnectionsPerPeer", String.valueOf(maxConnections));
    TransportConf conf = new TransportConf("rpc", new MapConfigProvider(configMap));

    RpcHandler rpcHandler = new NoOpRpcHandler();
    TransportContext context = new TransportContext(conf, rpcHandler);
    final TransportClientFactory factory = context.createClientFactory();
    final Set<TransportClient> clients = Collections.synchronizedSet(new HashSet<>());
    final AtomicInteger failed = new AtomicInteger();
    Thread[] attempts = new Thread[maxConnections * 10];

    for (int i = 0; i < attempts.length; ++i) {
      attempts[i] = new Thread() {
        @Override
        public void run() {
          try {
            TransportClient client = factory.createClient(AddressUtil.getLocalHost(), server1.getPort());
            assertTrue(client.isActive());
            clients.add(client);
          } catch (IOException e) {
            failed.incrementAndGet();
          }
        }
      };
      if (concurrent) {
        attempts[i].start();
      } else {
        attempts[i].run();
      }
    }

    for (int i = 0; i < attempts.length; ++i) {
      attempts[i].join();
    }

    assertEquals(0, failed.get());
//    assert clients.size() == maxConnections;
    for (TransportClient client : clients) {
      client.close();
    }
    factory.close();
  }

  @Test
  public void reuseClientsUpToConfigVariable() throws IOException, InterruptedException {
    testClientReuse(1, false);
    testClientReuse(2, false);
    testClientReuse(3,false);
    testClientReuse(4, false);
  }

  @Test
  public void reuseClientsUpToConfigVariableConcurrent() throws Exception {
    testClientReuse(1, true);
    testClientReuse(2, true);
    testClientReuse(3, true);
    testClientReuse(4, true);
  }

  @Test
  public void returnDifferentClientsForDifferentServers() throws Exception {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(AddressUtil.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(AddressUtil.getLocalHost(), server2.getPort());

    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    assertTrue(c1 != c2);
    factory.close();
  }

  @Test
  public void neverReturnInactiveClients() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(AddressUtil.getLocalHost(), server1.getPort());
    c1.close();
    long start = System.currentTimeMillis();
    while (c1.isActive() && (System.currentTimeMillis() - start) < 3000) {
      Thread.sleep(10);
    }
    assertFalse(c1.isActive());
    TransportClient c2 = factory.createClient(AddressUtil.getLocalHost(), server1.getPort());
    assertFalse(c1 == c2);
    assertTrue(c2.isActive());
    factory.close();
  }

  @Test
  public void closeBlockClientsWithFactory() throws IOException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(AddressUtil.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(AddressUtil.getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    factory.close();
    assertFalse(c1.isActive());
    assertFalse(c2.isActive());
  }

  @Test
  public void closeIdleConnectionForRequestTimeout() throws IOException, InterruptedException {
    TransportConf conf = new TransportConf("rpc", new ConfigProvider() {
      @Override public String get(String name) {
        if ("dts.rpc.io.connectionTimeout".equals(name)) {
          return "1000";
        }
        String value = System.getProperty(name);
        if (value == null) {
          throw new NoSuchElementException(name);
        }
        return value;
      }
    });
    TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
    TransportClientFactory factory = context.createClientFactory();
    try {
      TransportClient c1 = factory.createClient(AddressUtil.getLocalHost(), server1.getPort());
      assertTrue(c1.isActive());
      long expiredTime = System.currentTimeMillis() + 10000;
      while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
        Thread.sleep(10);
      }
      assertFalse(c1.isActive());
    } finally {
      factory.close();
    }
  }
}
