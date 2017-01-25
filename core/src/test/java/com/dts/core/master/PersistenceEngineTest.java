package com.dts.core.master;

import com.dts.core.util.Tuple2;
import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.RpcEndpointRef;
import com.dts.rpc.RpcEnv;
import com.dts.rpc.util.SerializerInstance;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author zhangxin
 */
public class PersistenceEngineTest {

  @Test
  public void testZooKeeperPersistenceEngine() throws Exception {
    DTSConf conf = new DTSConf(false);
    TestingServer zkTestServer = new TestingServer(findFreePort(conf));
    conf.set("dts.master.zookeeper.url", zkTestServer.getConnectString());
    try {
      SerializerInstance serializerInstance = new SerializerInstance(conf);
      ZooKeeperPersistenceEngine persistenceEngine = new ZooKeeperPersistenceEngine(conf, serializerInstance);
      testPersistenceEngine(conf, persistenceEngine);
    } finally {
      zkTestServer.stop();
    }
  }

  private void testPersistenceEngine(DTSConf conf, ZooKeeperPersistenceEngine persistenceEngine) {
    try {
      persistenceEngine.persist("test_1", "test_1_value");
      assertEquals(Lists.newArrayList("test_1_value"), persistenceEngine.read("test_"));
      persistenceEngine.persist("test_2", "test_2_value");
      assertEquals(Sets.newHashSet("test_1_value", "test_2_value"), Sets.newHashSet(persistenceEngine.read("test_")));
      persistenceEngine.unpersist("test_1");
      assertEquals(Lists.newArrayList("test_2_value"), persistenceEngine.read("test_"));
      persistenceEngine.unpersist("test_2");
      assertTrue(persistenceEngine.read("test_").size() == 0);

      RpcEnv testRpcEnv = RpcEnv.create("test", "localhost", 12345, conf, false);
      try {
        RpcEndpointRef workerEndpointRef = testRpcEnv.setupEndpoint("worker", new RpcEndpoint(testRpcEnv) {
          @Override public void receive(Object o) {
            // do nothing
          }
        });
        WorkerInfo workerToPersist = new WorkerInfo("test_worker", "test_group", workerEndpointRef);
        persistenceEngine.addWorker(workerToPersist);
        Tuple2<List<WorkerInfo>, List<ClientInfo>> tuple = persistenceEngine.readPersistedData(testRpcEnv);
        List<WorkerInfo> storedWorkers = tuple._1;
        List<ClientInfo> storedClients = tuple._2;

        assertEquals(0, storedClients.size());
        assertEquals(1, storedWorkers.size());
        WorkerInfo recoveryWorkerInfo = storedWorkers.get(0);

        assertEquals(workerToPersist.id, recoveryWorkerInfo.id);
        assertEquals(workerToPersist.host, recoveryWorkerInfo.host);
        assertEquals(workerToPersist.port, recoveryWorkerInfo.port);
        assertEquals(workerToPersist.endpoint, recoveryWorkerInfo.endpoint);
        assertEquals(workerToPersist.groupId, recoveryWorkerInfo.groupId);
      } finally {
        testRpcEnv.shutdown();
        testRpcEnv.awaitTermination();
      }
    } finally {
      persistenceEngine.close();
    }
  }

  private int findFreePort(DTSConf conf) {
    int candidatePort = RandomUtils.nextInt(1024, 65536);
    return candidatePort;
  }
}
