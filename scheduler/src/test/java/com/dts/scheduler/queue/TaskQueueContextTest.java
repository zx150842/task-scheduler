package com.dts.scheduler.queue;

import com.dts.core.DTSConf;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.util.IdUtil;
import com.dts.scheduler.MasterSource;
import com.dts.scheduler.queue.mysql.MysqlCronTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlExecutableTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlExecutingTaskQueue;
import com.dts.scheduler.queue.mysql.MysqlFinishedTaskQueue;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.*;

/**
 * @author zhangxin
 */
public class TaskQueueContextTest {

  private DTSConf conf = new DTSConf(false);
  private CronTaskQueue cronTaskQueue;
  private ExecutableTaskQueue executableTaskQueue;
  private ExecutingTaskQueue executingTaskQueue;
  private FinishedTaskQueue finishedTaskQueue;

  @Before
  public void setup() {
    cronTaskQueue = new MysqlCronTaskQueue(conf);
    executableTaskQueue = new MysqlExecutableTaskQueue();
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
      task.setTriggerTime(jobConf.getNextTriggerTime());
      executableTaskQueue.add(task);
      cronTaskQueue.triggeredJob(jobConf);
    }
  }

  @Test
  public void testCronTaskGenerator() {
    DTSConf conf = new DTSConf(false);
    TaskQueueContext.CronTaskGenerator generator = new TaskQueueContext(conf).new CronTaskGenerator();
    generator.run();
  }

  @Test
  public void testJDBCSave() {
    String url = "jdbc:mysql://test.adstream.rds.sogou/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai";
    String user = "adstream_w";
    String pwd = "ad63f80e1986";
    try {
      Class.forName("com.mysql.jdbc.Driver");
      Connection conn = DriverManager.getConnection(url, user, pwd);
      Statement stmt = conn.createStatement();
//      PreparedStatement pstmt = conn.prepareStatement("insert into dts_cron_jobs"
//        + "(job_id,tasks,cron_expression,worker_group,status,last_trigger_time)"
//        + "values(?,?,?,?,?,?)");
//      pstmt.setString(1, "testJobId");
//      pstmt.setString(2, "test");
//      pstmt.setString(3, "20 */30 * * * ?");
//      pstmt.setString(4, "workerGroup");
//      pstmt.setInt(5, 0);
//      pstmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
//      pstmt.execute();
      PreparedStatement pstmt = conn.prepareStatement("select last_trigger_time from dts_cron_jobs where job_id='testJobId'");
      ResultSet rs = pstmt.executeQuery();
      while (rs.next()) {
        System.out.println(rs.getTimestamp(1));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
