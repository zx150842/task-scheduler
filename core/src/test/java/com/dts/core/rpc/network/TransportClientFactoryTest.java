package com.dts.core.rpc.network;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
    server1 = context.createServer(TestUtils.getLocalHost(), 8000);
    server2 = context.createServer(TestUtils.getLocalHost(), 8001);
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
            TransportClient client = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
            assert client.isActive();
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

    assert failed.get() == 0;
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
    TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());

    assert c1.isActive();
    assert c2.isActive();
    assert c1 != c2;
    factory.close();
  }
}
