package com.dts.scheduler;

import com.google.common.collect.Lists;

import com.dts.core.DTSConf;
import com.dts.core.rpc.RpcAddress;
import com.dts.core.rpc.RpcEnv;
import com.dts.scheduler.queue.TaskQueueContext;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class TestMaster extends Master {
  public final CountDownLatch masterLatch = new CountDownLatch(1);
  public final List messages = Lists.newArrayList();

  public TestMaster(RpcEnv rpcEnv, RpcAddress address, DTSConf conf,
    TaskQueueContext taskQueueContext) {
    super(rpcEnv, address, conf);
  }

  @Override
  public void receive(Object o, RpcAddress senderAddress) {
    if (o instanceof Heartbeat) {
      messages.add(o);
      masterLatch.countDown();
    }

    else if (o instanceof WorkerLastestState) {
      messages.add(o);
    }

    else if (o instanceof LaunchedTask) {
      messages.add(o);
    }

    else if (o instanceof ExecutingTask) {
      messages.add(o);
    }

    else if (o instanceof FinishTask) {
      messages.add(o);
    }

    else if (o instanceof ManualTriggerJob) {
      messages.add(o);
    }
  }
}
