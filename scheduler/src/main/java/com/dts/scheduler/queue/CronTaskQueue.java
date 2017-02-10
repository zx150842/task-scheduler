package com.dts.scheduler.queue;

import com.dts.core.JobConf;
import com.dts.core.TaskConf;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhangxin
 */
public interface CronTaskQueue {

  List<JobConf> getAllValid();

  boolean containJob(String jobId);

  JobConf getJob(String jobId);

  TaskConf getNextToTriggerTask(String jobId, String taskName);

  List<JobConf> getNextTriggerJobs(long noEarlyThan, long noLaterThan);

  void triggeredJob(JobConf jobConf);

  ReentrantLock triggerJobLock();
}
