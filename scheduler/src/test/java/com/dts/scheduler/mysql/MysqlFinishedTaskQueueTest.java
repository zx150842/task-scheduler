package com.dts.scheduler.mysql;

import com.dts.core.TriggeredTaskInfo;
import com.dts.scheduler.queue.mysql.MysqlExecutingTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlFinishedTaskQueue;

import org.junit.Before;
import org.junit.Test;

/**
 * @author zhangxin
 */
public class MysqlFinishedTaskQueueTest {
  private MysqlFinishedTaskQueue mysqlFinishedTaskQueue;
  private TriggeredTaskInfo task;

  @Before
  public void setup() {
    mysqlFinishedTaskQueue = new MysqlFinishedTaskQueue();
    task = new TriggeredTaskInfo("test_jobId", "test_group",
      "test_taskId", "test_taskName", "test_param", "test_sysId", true);
  }

  @Test
  public void testAdd() {
    TriggeredTaskInfo t = new MysqlExecutingTaskQueue().getBySysId("1486447586760");
    mysqlFinishedTaskQueue.add(t);
  }
}
