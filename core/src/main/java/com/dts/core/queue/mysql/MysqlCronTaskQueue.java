package com.dts.core.queue.mysql;

import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.queue.CronTaskQueue;
import com.dts.core.queue.mysql.impl.AbstractSqlQueue;
import com.dts.core.queue.mysql.impl.CronJob;
import com.dts.core.queue.mysql.impl.CronJobDao;
import com.dts.core.util.CronExpressionUtil;
import com.dts.core.util.MybatisUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.rpc.DTSConf;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.ibatis.session.SqlSession;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangxin
 */
public class MysqlCronTaskQueue extends AbstractSqlQueue implements CronTaskQueue {
  private static final String PREFIX = CronJobDao.class.getName();
  private static final Gson GSON = new Gson();
  private static final ReentrantLock lock = new ReentrantLock();

  private final ScheduledExecutorService backend;

  private Map<String, JobConf> jobs = Maps.newHashMap();
  private TreeSet<JobConf> triggerJobs;

  public MysqlCronTaskQueue(DTSConf conf) {
    long cronJobRefreshSec = conf.getLong("dts.master.cronJob.refreshSec", 300);
    refreshJobMap();
    backend = ThreadUtil.newDaemonSingleThreadScheduledExecutor("cron-job-refresh");
    backend.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        refreshJobMap();
      }
    }, cronJobRefreshSec, cronJobRefreshSec, TimeUnit.SECONDS);
  }

  @Override public List<JobConf> getAllValid() {
    return Lists.newArrayList(jobs.values());
  }

  @Override
  public List<JobConf> getNextTriggerJobs(long noLaterThan) {
    List<JobConf> toTriggerJobs = Lists.newArrayList();
    try {
      lock.lock();
      // TODO add min job
      for (JobConf jobConf : triggerJobs) {
        if (jobConf.getNextTriggerTime().getTime() > noLaterThan) {
          break;
        }
        toTriggerJobs.add(jobConf);
        triggerJobs.remove(jobConf);
      }
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
    return toTriggerJobs;
  }

  public void triggeredJob(JobConf jobConf) {
    SqlSession sqlSession = null;
    try {
      Date lastTriggerTime = jobConf.getNextTriggerTime();
      Date nextTriggerTime = CronExpressionUtil.getNextTriggerTime(jobConf.getCronExpression(), lastTriggerTime);
      sqlSession = MybatisUtil.getSqlSession();
      Map<String, Object> params = Maps.newHashMap();
      params.put("jobId", jobConf.getJobId());
      params.put("lastTriggerTime", lastTriggerTime);
      sqlSession.update(PREFIX + ".updateTriggerTime", params);
      sqlSession.commit();
      jobConf.setLastTriggerTime(lastTriggerTime);
      jobConf.setNextTriggerTime(nextTriggerTime);
      lock.lock();
      triggerJobs.add(jobConf);
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }

  @Override public boolean containJob(String jobId) {
    return jobs.containsKey(jobId);
  }

  @Override public JobConf getJob(String jobId) {
    return jobs.get(jobId);
  }

  @Override public TaskConf getNextToTriggerTask(String jobId, String taskId) {
    JobConf jobConf = jobs.get(jobId);
    for (int i = 0; i < jobConf.getTasks().size(); ++i) {
      TaskConf taskConf = jobConf.getTasks().get(i);
      if (taskConf.getTaskId().equals(taskId) && i != jobConf.getTasks().size() - 1) {
        return jobConf.getTasks().get(i + 1);
      }
    }
    return null;
  }

  private void refreshJobMap() {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      List<CronJob> cronJobs = sqlSession.selectList(PREFIX + ".getAllValid");
      Map<String, JobConf> map = Maps.newHashMap();
      TreeSet<JobConf> set = Sets.newTreeSet(new JobTriggerComparator());
      for (CronJob cronJob : cronJobs) {
        JobConf jobConf = deserializeCronJob(cronJob);
        // 当从数据库刷新缓存时，初始化下次触发时间
        Date nextTriggerTime = CronExpressionUtil.getNextTriggerTime(jobConf.getCronExpression(),
          jobConf.getLastTriggerTime());
        jobConf.setNextTriggerTime(nextTriggerTime);
        map.put(cronJob.getJobId(), deserializeCronJob(cronJob));
        set.add(jobConf);
      }
      lock.lock();
      jobs = map;
      triggerJobs = set;
    } catch (Exception e) {
      throw e;
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }

  private JobConf deserializeCronJob(CronJob cronJob) {
    List<TaskConf> tasks = GSON.fromJson(cronJob.getTasks(), new TypeToken<List<TaskConf>>(){}.getType());
    return new JobConf(cronJob.getJobId(), cronJob.getCronExpression(), cronJob.getWorkerGroup(),
      cronJob.getMaxRunTimeSec(), cronJob.getLastTriggerTime(), tasks);
  }

  class JobTriggerComparator implements Comparator<JobConf> {
    @Override public int compare(JobConf o1, JobConf o2) {
      if (o1.getNextTriggerTime().before(o2.getNextTriggerTime())) {
        return -1;
      }
      if (o1.getNextTriggerTime().after(o2.getNextTriggerTime())) {
        return 1;
      }
      return o1.getJobId().compareTo(o2.getJobId());
    }
  }
}
