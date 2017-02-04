package com.dts.scheduler.queue.mysql.impl;

import java.sql.Timestamp;

/**
 * @author zhangxin
 */
public class CronTask {
  // task所属的jobId
  private String jobId;
  // 运行task的worker组
  private String workerGroup;
  // task的唯一标识
  private String taskId;
  // task name
  private String taskName;
  // task参数
  private String params;

  // task的系统内部id
  private String sysId;

  // 是否是手动触发
  private boolean manualTrigger;

  private int status;
  // task被分配运行的workerId
  private String workerId;
  // task被分配运行的线程名
  private String threadName;
  private Timestamp executableTime;
  private Timestamp launchingTime;
  private Timestamp launchedTime;
  private Timestamp executingTime;

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getWorkerGroup() {
    return workerGroup;
  }

  public void setWorkerGroup(String workerGroup) {
    this.workerGroup = workerGroup;
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

  public String getSysId() {
    return sysId;
  }

  public void setSysId(String sysId) {
    this.sysId = sysId;
  }

  public boolean isManualTrigger() {
    return manualTrigger;
  }

  public void setManualTrigger(boolean manualTrigger) {
    this.manualTrigger = manualTrigger;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getWorkerId() {
    return workerId;
  }

  public void setWorkerId(String workerId) {
    this.workerId = workerId;
  }

  public String getThreadName() {
    return threadName;
  }

  public void setThreadName(String threadName) {
    this.threadName = threadName;
  }

  public Timestamp getExecutableTime() {
    return executableTime;
  }

  public void setExecutableTime(Timestamp executableTime) {
    this.executableTime = executableTime;
  }

  public Timestamp getLaunchingTime() {
    return launchingTime;
  }

  public void setLaunchingTime(Timestamp launchingTime) {
    this.launchingTime = launchingTime;
  }

  public Timestamp getLaunchedTime() {
    return launchedTime;
  }

  public void setLaunchedTime(Timestamp launchedTime) {
    this.launchedTime = launchedTime;
  }

  public Timestamp getExecutingTime() {
    return executingTime;
  }

  public void setExecutingTime(Timestamp executingTime) {
    this.executingTime = executingTime;
  }
}
