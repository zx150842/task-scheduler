package com.dts.scheduler.queue.mysql;

import com.dts.core.metrics.MetricsSystem;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.dts.core.DTSConf;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.util.CronExpressionUtil;
import com.dts.core.util.ThreadUtil;
import com.dts.scheduler.MybatisUtil;
import com.dts.scheduler.queue.CronTaskQueue;
import com.dts.scheduler.queue.mysql.vo.CronJob;
import com.dts.scheduler.queue.mysql.dao.CronJobDao;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangxin
 */
public class MysqlCronTaskQueue extends AbstractSqlQueue implements CronTaskQueue {
  private final Logger logger = LoggerFactory.getLogger(MysqlCronTaskQueue.class);
  private static final String PREFIX = CronJobDao.class.getName();
  private static final Gson GSON = new Gson();
  private static final ReentrantLock lock = new ReentrantLock();

  private final ScheduledExecutorService backend;

  private Map<String, JobConf> jobs = Maps.newHashMap();
  private TreeSet<JobConf> triggerJobs;

  private final MysqlQueueSource mysqlQueueSource;

  public MysqlCronTaskQueue(DTSConf conf) {
    this.mysqlQueueSource = new MysqlQueueSource();
    MetricsSystem.createMetricsSystem(conf).registerSource(mysqlQueueSource);
    long cronJobRefreshSec = conf.getLong("dts.master.cronJob.refreshSec", 300);
    refreshJobs();
    backend = ThreadUtil.newDaemonSingleThreadScheduledExecutor("cron-job-refresh");
    backend.scheduleAtFixedRate(() -> refreshJobs(), cronJobRefreshSec, cronJobRefreshSec, TimeUnit.SECONDS);
  }

  @Override public List<JobConf> getAllValid() {
    return Lists.newArrayList(jobs.values());
  }

  @Override
  public List<JobConf> getNextTriggerJobs(long noEarlyThan, long noLaterThan) {
    List<JobConf> toTriggerJobs = Lists.newArrayList();
    Date now = new Date();
    for (JobConf jobConf : triggerJobs) {
      if (jobConf.getNextTriggerTime().getTime() > noLaterThan) {
        break;
      }
      if (jobConf.getNextTriggerTime().getTime() < noEarlyThan) {
        jobConf.setNextTriggerTime(now);
        logger.warn("Job:{} next trigger time: {} is too early, reset to {}",
            jobConf.getJobId(), jobConf.getNextTriggerTime(), now);
      }
      toTriggerJobs.add(jobConf);
    }
    triggerJobs.removeAll(toTriggerJobs);
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
      triggerJobs.add(jobConf);
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }

  @Override public boolean containJob(String jobId) {
    return jobs.containsKey(jobId);
  }

  @Override public JobConf getJob(String jobId) {
    JobConf jobConf = jobs.get(jobId);
    if (jobConf == null) {
      jobConf = getJobFromDB(jobId);
    }
    return jobConf;
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

  JobConf getJobFromDB(String jobId) {
    SqlSession sqlSession = null;
    JobConf jobConf = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      List<CronJob> jobs = sqlSession.selectList(PREFIX + ".getByJobId", jobId);
      if (jobs != null && !jobs.isEmpty()) {
        jobConf = deserializeCronJob(jobs.get(0));
      }
    } catch (Exception e) {
      logger.error(Throwables.getStackTraceAsString(e));
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
    return jobConf;
  }

  public ReentrantLock triggerJobLock() {
    return lock;
  }

  public boolean refreshJobs() {
    mysqlQueueSource.refreshJobMeter.mark();
    SqlSession sqlSession = null;
    boolean success;
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
      success = true;
    } catch (Exception e) {
      logger.error(Throwables.getStackTraceAsString(e));
      success = false;
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
      MybatisUtil.closeSqlSession(sqlSession);
    }
    return success;
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
