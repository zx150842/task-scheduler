package com.dts.executor;

import com.dts.core.DTSConf;
import com.dts.core.DeployMessages;
import com.dts.core.EndpointNames;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.executor.task.TaskMethodWrapper;
import com.dts.executor.task.TaskScanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author zhangxin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:applicationContext.xml")
public class WorkerTest {

  @Autowired
  private TaskScanner taskScanner;

  private Worker _worker;
  private RpcEndpointRef _workerRef;

  @Before
  public void setup() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    String host = AddressUtil.getLocalHost();
    int port = conf.getInt("dts.worker.port", 10000);
    String packageName = conf.get("dts.worker.packageName");
    TaskMethodWrapper tw = taskScanner.getTaskMethodWrapper(packageName);
    _worker = Worker.launchWorker(host, port, tw, conf);
    _workerRef = new NettyRpcEndpointRef(conf,
        new RpcEndpointAddress(host, port, EndpointNames.WORKER_ENDPOINT),
        (NettyRpcEnv) _worker.rpcEnv());
  }

  @After
  public void tearDown() {
    _worker.stop();
  }

  @Test
  public void testLaunchTask() throws Exception {
    String jobId = "jobId";
    String workerGroup = "testGroup";
    String taskId = "taskId";
    String taskName = "task1";
    String params = "hello world, 1";
    String sysId = "1";
    TriggeredTaskInfo task = new TriggeredTaskInfo(jobId, workerGroup, taskId,
        taskName, params, sysId, false);
    Future<DeployMessages.LaunchedTask> future = _workerRef.ask(new DeployMessages.LaunchTask(task));
    DeployMessages.LaunchedTask message = future.get();
    assertEquals(message.message, "success");
  }

  @Test
  public void testStartWorker() throws InterruptedException {
    TimeUnit.SECONDS.sleep(100);
  }
}
