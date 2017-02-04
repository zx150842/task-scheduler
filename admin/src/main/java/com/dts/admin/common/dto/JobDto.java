package com.dts.admin.common.dto;

import java.util.Date;

/**
 * @author zhangxin
 */
public class JobDto {
  private String jobId;
  private String taskId;
  private String taskName;
  private String params;
  // cron表达式
  private String cronExpression;
  // job提交到的worker组
  private String workerGroup;
  // job状态
  private int status;
  // job允许运行的最大时间（s）
  private long maxRunTimeSec;
  private Date lastTriggerTime;
  private String desc;

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
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

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }
}
