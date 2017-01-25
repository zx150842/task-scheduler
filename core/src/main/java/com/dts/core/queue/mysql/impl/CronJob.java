package com.dts.core.queue.mysql.impl;

import java.util.Date;

/**
 * @author zhangxin
 */
public class CronJob {
  private String jobId;
  // task名字
  private String tasks;
  // cron表达式
  private String cronExpression;
  // job提交到的worker组
  private String workerGroup;
  // job状态
  private int status;
  // job允许运行的最大时间（s）
  private long maxRunTimeSec;
  private Date lastTriggerTime;

  public CronJob() {}

  public CronJob(String jobId, String tasks, String cronExpression,
    String workerGroup, int status, long maxRunTimeSec, Date lastTriggerTime) {
    this.jobId = jobId;
    this.tasks = tasks;
    this.cronExpression = cronExpression;
    this.workerGroup = workerGroup;
    this.status = status;
    this.maxRunTimeSec = maxRunTimeSec;
    this.lastTriggerTime = lastTriggerTime;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getTasks() {
    return tasks;
  }

  public void setTasks(String tasks) {
    this.tasks = tasks;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public void setCronExpression(String cronExpression) {
    this.cronExpression = cronExpression;
  }

  public String getWorkerGroup() {
    return workerGroup;
  }

  public void setWorkerGroup(String workerGroup) {
    this.workerGroup = workerGroup;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public long getMaxRunTimeSec() {
    return maxRunTimeSec;
  }

  public void setMaxRunTimeSec(long maxRunTimeSec) {
    this.maxRunTimeSec = maxRunTimeSec;
  }

  public Date getLastTriggerTime() {
    return lastTriggerTime;
  }

  public void setLastTriggerTime(Date lastTriggerTime) {
    this.lastTriggerTime = lastTriggerTime;
  }
}
