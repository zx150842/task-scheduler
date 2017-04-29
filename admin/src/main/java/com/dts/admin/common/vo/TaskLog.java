package com.dts.admin.common.vo;

import java.sql.Timestamp;

/**
 * @author zhangxin
 */
public class TaskLog {
  private int id;
  private String jobId;
  private String taskId;
  private String taskName;
  private String workerGroup;
  private String sysId;
  private String params;
  private boolean manualTrigger;
  private String workerId;
  private Timestamp executableTime;
  private Timestamp executingTime;
  private Timestamp finishTime;
  private Timestamp ctime;
  private String executeResult;
  private int status;
  private int resumes;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

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

  public String getWorkerGroup() {
    return workerGroup;
  }

  public void setWorkerGroup(String workerGroup) {
    this.workerGroup = workerGroup;
  }

  public String getSysId() {
    return sysId;
  }

  public void setSysId(String sysId) {
    this.sysId = sysId;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public boolean isManualTrigger() {
    return manualTrigger;
  }

  public void setManualTrigger(boolean manualTrigger) {
    this.manualTrigger = manualTrigger;
  }

  public String getWorkerId() {
    return workerId;
  }

  public void setWorkerId(String workerId) {
    this.workerId = workerId;
  }

  public Timestamp getExecutableTime() {
    return executableTime;
  }

  public void setExecutableTime(Timestamp executableTime) {
    this.executableTime = executableTime;
  }

  public Timestamp getExecutingTime() {
    return executingTime;
  }

  public void setExecutingTime(Timestamp executingTime) {
    this.executingTime = executingTime;
  }

  public Timestamp getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(Timestamp finishTime) {
    this.finishTime = finishTime;
  }

  public Timestamp getCtime() {
    return ctime;
  }

  public void setCtime(Timestamp ctime) {
    this.ctime = ctime;
  }

  public String getExecuteResult() {
    return executeResult;
  }

  public void setExecuteResult(String executeResult) {
    this.executeResult = executeResult;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public int getResumes() {
    return resumes;
  }

  public void setResumes(int resumes) {
    this.resumes = resumes;
  }
}
