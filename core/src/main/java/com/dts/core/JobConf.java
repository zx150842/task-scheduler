package com.dts.core;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * @author zhangxin
 */
public class JobConf implements Serializable {
  private String jobId;
  // 一个job中所有的task
  private List<TaskConf> tasks;
  // cron表达式
  private String cronExpression;
  // job提交到的worker组
  private String workerGroup;
  // job状态
  private int status;
  // job允许运行的最大时间（s）
  private long maxRunTimeSec = -1;
  // job最后被触发的时间
  private Date lastTriggerTime;

  transient private Date nextTriggerTime;

  public JobConf() {}

  public JobConf(String jobId, String cronExpression, String workerGroup, long maxRunTimeSec, Date lastTriggerTime, TaskConf task) {
    this(jobId, cronExpression, workerGroup, maxRunTimeSec, lastTriggerTime, Lists.newArrayList(task));
  }

  public JobConf(String jobId, String cronExpression, String workerGroup, long maxRunTimeSec, Date lastTriggerTime, List<TaskConf> tasks) {
    this.jobId = jobId;
    this.cronExpression = cronExpression;
    this.workerGroup = workerGroup;
    this.maxRunTimeSec = maxRunTimeSec;
    this.tasks = tasks;
    this.lastTriggerTime = lastTriggerTime;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public List<TaskConf> getTasks() {
    return tasks;
  }

  public void setTasks(List<TaskConf> tasks) {
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

  public Date getNextTriggerTime() {
    return nextTriggerTime;
  }

  public void setNextTriggerTime(Date nextTriggerTime) {
    this.nextTriggerTime = nextTriggerTime;
  }
}
