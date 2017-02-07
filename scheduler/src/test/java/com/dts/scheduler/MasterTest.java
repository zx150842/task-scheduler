package com.dts.scheduler;

import com.dts.core.EndpointNames;
import com.dts.core.DTSConf;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.scheduler.queue.TaskQueueContext;

import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class MasterTest {

  private Master _master;

  @Before
  public void setup() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    _master = Master.launchMaster(conf);
  }

  @After
  public void tearDown() {
    _master.stop();
  }

  @Test
  public void testElectedLeader() throws InterruptedException {
    TimeUnit.SECONDS.sleep(100);
  }

  @Test
  public void testSendTask() {
    String workerGroup = "executor-example";
    _master.schedule(workerGroup);
  }

}
