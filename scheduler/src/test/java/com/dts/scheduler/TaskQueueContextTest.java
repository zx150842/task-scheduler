package com.dts.scheduler;

import com.dts.core.DTSConf;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.util.IdUtil;
import com.dts.scheduler.queue.CronTaskQueue;
import com.dts.scheduler.queue.ExecutableTaskQueue;
import com.dts.scheduler.queue.ExecutingTaskQueue;
import com.dts.scheduler.queue.FinishedTaskQueue;
import com.dts.scheduler.queue.LaunchedTaskQueue;
import com.dts.scheduler.queue.LaunchingTaskQueue;
import com.dts.scheduler.queue.TaskQueueContext;
import com.dts.scheduler.queue.mysql.MysqlCronTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlExecutableTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlExecutingTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlFinishedTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlLaunchedTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlLaunchingTaskQueue;

import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;

/**
 * @author zhangxin
 */
public class TaskQueueContextTest {

  private DTSConf conf = new DTSConf(false);
  private CronTaskQueue cronTaskQueue;
  private ExecutableTaskQueue executableTaskQueue;
  private LaunchingTaskQueue launchingTaskQueue;
  private LaunchedTaskQueue launchedTaskQueue;
  private ExecutingTaskQueue executingTaskQueue;
  private FinishedTaskQueue finishedTaskQueue;

  @Before
  public void setup() {
    cronTaskQueue = new MysqlCronTaskQueue(conf);
    executableTaskQueue = new MysqlExecutableTaskQueue();
    launchingTaskQueue = new MysqlLaunchingTaskQueue();
    launchedTaskQueue = new MysqlLaunchedTaskQueue();
    executingTaskQueue = new MysqlExecutingTaskQueue();
    finishedTaskQueue = new MysqlFinishedTaskQueue();
  }

  @Test
  public void testTrigger() {
    long now = System.currentTimeMillis();
    List<JobConf> jobConfs = cronTaskQueue.getNextTriggerJobs(now - 1000, now + 100);
    for (JobConf jobConf : jobConfs) {
      TaskConf taskConf = jobConf.getTasks().get(0);
      System.out.println(taskConf);
      TriggeredTaskInfo task = new TriggeredTaskInfo(jobConf.getJobId(), jobConf.getWorkerGroup(),
          taskConf.getTaskId(), taskConf.getTaskName(), taskConf.getParams(), IdUtil.getUniqId(), false);
      task.setTriggerTime(new Timestamp(jobConf.getNextTriggerTime().getTime()));
      executableTaskQueue.add(task);
      cronTaskQueue.triggeredJob(jobConf);
    }
  }
}
