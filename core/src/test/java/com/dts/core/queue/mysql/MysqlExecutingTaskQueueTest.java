package com.dts.core.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author zhangxin
 */
public class MysqlExecutingTaskQueueTest {
  private MysqlExecutingTaskQueue mysqlExecutingTaskQueue;
  private TriggeredTaskInfo task;

  @Before
  public void setup() {
    mysqlExecutingTaskQueue = new MysqlExecutingTaskQueue();
    task = new TriggeredTaskInfo("test_jobId", "test_group",
      "test_taskId", "test_taskName", "test_param", "test_sysId", true);
  }

  @Test
  public void testAdd() {
    mysqlExecutingTaskQueue.add(task);
  }

  @Test
  public void testRemove() {
    mysqlExecutingTaskQueue.remove(task.getSysId());
  }

  @Test
  public void testGetByTaskId() {
    List<TriggeredTaskInfo> tasks = mysqlExecutingTaskQueue.getByTaskId(task.getTaskId());
    for (TriggeredTaskInfo task : tasks) {
      System.out.println(task);
    }
  }
}
