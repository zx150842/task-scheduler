package com.dts.core.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author zhangxin
 */
public class MysqlExecutableTaskQueueTest {
  private MysqlExecutableTaskQueue mysqlExecutableTaskQueue;
  private TriggeredTaskInfo task;

  @Before
  public void setup() {
    mysqlExecutableTaskQueue = new MysqlExecutableTaskQueue();
    task = new TriggeredTaskInfo("test_jobId", "test_group",
      "test_taskId", "test_taskName", "test_param", "test_sysId", true);
  }

  @Test
  public void testGetManualTriggerTasks() {
    List<TriggeredTaskInfo> tasks = mysqlExecutableTaskQueue.getManualTriggerTasks("test_group");
    for (TriggeredTaskInfo task : tasks) {
      System.out.println(task);
    }
  }

  @Test
  public void testGetAutoTriggerTasks() {
    List<TriggeredTaskInfo> tasks = mysqlExecutableTaskQueue.getAutoTriggerTasks("test_group");
    for (TriggeredTaskInfo task : tasks) {
      System.out.println(task);
    }
  }

  @Test
  public void testAdd() {
    mysqlExecutableTaskQueue.add(task);
  }

}
