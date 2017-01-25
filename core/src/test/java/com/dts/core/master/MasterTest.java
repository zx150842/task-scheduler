package com.dts.core.master;

import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.TestUtils;
import com.dts.core.queue.TaskQueueContext;
import com.dts.core.worker.Worker;
import com.dts.rpc.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.*;

import static com.dts.core.DeployMessages.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author zhangxin
 */
public class MasterTest {

  private RpcEnv _masterEnv;
  private RpcEnv _workerEnv;

  private RpcEndpointRef _masterRef;
  private RpcEndpointRef _workerRef;

  private TestingServer _zkTestServer;

  @After
  public void tearDown() throws IOException {
    if (_masterEnv != null) {
      _masterEnv.shutdown();
      _masterEnv.awaitTermination();
      _masterEnv = null;
    }
    if (_workerEnv != null) {
      _workerEnv.shutdown();
      _workerEnv.awaitTermination();
      _workerEnv = null;
    }
    if (_zkTestServer != null) {
      _zkTestServer.close();
    }
  }

  @Test
  public void testElectedLeader() {
    makeMaster();
    _masterEnv.awaitTermination();
  }

  @Test
  public void testRegisterWorker()
    throws InterruptedException, ExecutionException, TimeoutException {
    makeMaster();
    Object result = makeWorker();
    assertTrue(result instanceof RegisteredWorker);
  }

  @Test
  public void testHeartbeat() throws Exception {
    TestMaster master = makeMaster();
    makeWorker();
    Heartbeat msg = new Heartbeat("workerId", _workerRef);
    _masterRef.send(msg);
    master.masterLatch.await();
    Heartbeat masterRecv = (Heartbeat) master.messages.get(0);
    assertTrue(msg.workerId.equals(masterRecv.workerId) && msg.worker.equals(masterRecv.worker));
  }

  @Test
  public void testRegisterJob() throws Exception {
    makeMaster();
    makeWorker();
    TaskConf taskConf = new TaskConf("test_task_id", "test_task", null);
    // TODO generate jobId
    JobConf jobConf = new JobConf("test_job", "20 */30 * * * ?", "test_worker_group", 1000, new Date(), taskConf);
    Future future = _masterRef.ask(new RegisterJob(jobConf));
    Object result = future.get();
    assertTrue(result instanceof RegisteredJob);
    assertEquals("test_job", ((RegisteredJob)result).jobId);
  }

  @Test
  public void testUnregisterJob() throws Exception {
    makeMaster();
    makeWorker();
    TaskConf taskConf = new TaskConf("test_task_id", "test_task", null);
    // TODO generate jobId
    JobConf jobConf = new JobConf("test_job", "20 */30 * * * ?", "test_worker_group", 1000, new Date(), taskConf);
    Future future1 = _masterRef.ask(new RegisterJob(jobConf));
    future1.get();

    Future future2 = _masterRef.ask(new UnregisterJob("test_job"));
    Object result = future2.get();
    assertTrue(result instanceof UnregisteredJob);
    assertEquals("test_job", ((UnregisteredJob)result).jobId);
  }

  @Test
  public void testUpdateJob() {

  }

  protected RpcEnv createRpcEnv(DTSConf conf, String name, int port, boolean clientMode) {
    return RpcEnv.create(name, TestUtils.getLocalHost(), port, conf, clientMode);
  }

  private TestMaster makeMaster() {
    assertTrue("Some Master's RpcEnv is leaked in tests", _masterEnv == null);
    DTSConf conf = new DTSConf(false);
      try {
        TestingServer zkTestServer = new TestingServer(findFreePort(conf));
        conf.set("dts.master.zookeeper.url", zkTestServer.getConnectString());
        conf.set("dts.master.queue.type", "memory");
        RpcEnv rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, TestUtils.getLocalHost(), 0, conf, false);
        TaskQueueContext context = new TaskQueueContext(conf);
        TestMaster master =  new TestMaster(rpcEnv, rpcEnv.address(), conf, context);
        rpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master);
        _masterEnv = rpcEnv;
        _zkTestServer = zkTestServer;
        return master;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private WorkerInfo makeWorkerInfo(int memoryMb, int cores) {
    String workerId = String.valueOf(System.currentTimeMillis());
    return new WorkerInfo(workerId, "test_group", null);
  }

  private Object makeWorker() {
    DTSConf conf = new DTSConf(false);
    RpcEnv workerEnv = createRpcEnv(conf, "worker", 0, false);
    _workerRef = workerEnv.setupEndpoint(Worker.ENDPOINT_NAME, mock(Worker.class));
    _masterRef = workerEnv.setupEndpointRef(_masterEnv.address(), Master.ENDPOINT_NAME);

    Future future = _masterRef.ask(new RegisterWorker("workerId", _workerRef, 4, 10000, "test_group"));
    Object result;
    try {
      result = future.get(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      result = e;
    }
    return result;
  }

  private int findFreePort(DTSConf conf) {
    int candidatePort = RandomUtils.nextInt(1024, 65536);
    return candidatePort;
  }
}
