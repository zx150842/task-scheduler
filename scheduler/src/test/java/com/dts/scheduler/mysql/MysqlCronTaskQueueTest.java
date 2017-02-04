package com.dts.scheduler.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.dts.core.DTSConf;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.scheduler.MybatisUtil;
import com.dts.scheduler.queue.mysql.MysqlCronTaskQueue;
import com.dts.scheduler.queue.mysql.impl.CronJobDao;

import org.apache.ibatis.session.SqlSession;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhangxin
 */
public class MysqlCronTaskQueueTest {

  private MysqlCronTaskQueue mysqlCronTaskQueue;

  private JobConf jobConf;

  @Before
  public void setup() {
    DTSConf conf = new DTSConf(false);
    mysqlCronTaskQueue = new MysqlCronTaskQueue(conf);

    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
    map.put("String", "testParam1");
    List<TaskConf> tasks = Lists.newArrayList();
    tasks.add(new TaskConf("testTaskId", "testTaskName", map.toString()));
    jobConf = new JobConf("testId", "20 */30 * * * ?", "testGroup", 100, new Date(), tasks);
    jobConf.setNextTriggerTime(new Date());
  }

  @Test
  public void testGetAllValid() {
    List<JobConf> list = mysqlCronTaskQueue.getAllValid();
    for (JobConf conf : list) {
      System.out.println(conf);
    }
  }

  @Test
  public void testTriggeredJob() {
    mysqlCronTaskQueue.triggeredJob(jobConf);
  }

  @Test
  public void test() {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      Map<String, Object> params = Maps.newHashMap();
      params.put("jobId", jobConf.getJobId());
      params.put("lastTriggerTime", new Timestamp(System.currentTimeMillis()));
      sqlSession.update(CronJobDao.class.getName() + ".updateTriggerTime", params);
      sqlSession.commit();
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }
}
