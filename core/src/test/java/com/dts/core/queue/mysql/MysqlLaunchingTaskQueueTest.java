package com.dts.core.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import org.junit.Before;
import org.junit.Test;

/**
 * @author zhangxin
 */
public class MysqlLaunchingTaskQueueTest {
  private MysqlLaunchingTaskQueue mysqlLaunchingTaskQueue;
  private TriggeredTaskInfo task;

  @Before
  public void setup() {
    mysqlLaunchingTaskQueue = new MysqlLaunchingTaskQueue();
    task = new TriggeredTaskInfo("test_jobId", "test_group",
      "test_taskId", "test_taskName", "test_param", "test_sysId", true);
  }

  @Test
  public void testAdd() {
    mysqlLaunchingTaskQueue.add(task);
  }
}
