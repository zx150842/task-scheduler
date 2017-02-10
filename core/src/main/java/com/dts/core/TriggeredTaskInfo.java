package com.dts.core;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * 被触发的task信息
 *
 * @author zhangxin
 */
public class TriggeredTaskInfo implements Serializable {
  private static final long serialVersionUID = 3918545175313075373L;
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
  transient private boolean manualTrigger;

  transient private int status;
  // task被分配运行的workerId
  transient private String workerId;
  // task被分配运行的线程名
  transient private String threadName;

  transient private Date triggerTime;
  transient private Date executableTime;
  transient private Date executingTime;
  transient private Date finishTime;
  transient private Date nextTriggerTime;
  transient private String executeResult;

  public TriggeredTaskInfo() {}

  public TriggeredTaskInfo(
    String jobId,
    String workerGroup,
    String taskId,
    String taskName,
    String params,
    String sysId,
    boolean manualTrigger) {
    this.jobId = jobId;
    this.workerGroup = workerGroup;
    this.taskId = taskId;
    this.taskName = taskName;
    this.params = params;
    this.sysId = sysId;
    this.manualTrigger = manualTrigger;
  }

  public String getJobId() {
    return jobId;
  }

  public String getWorkerGroup() {
    return workerGroup;
  }

  public String getTaskId() {
    return taskId;
  }

  public String getTaskName() {
    return taskName;
  }

  public String getParams() {
    return params;
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

  public Date getTriggerTime() {
    return triggerTime;
  }

  public void setTriggerTime(Date triggerTime) {
    this.triggerTime = triggerTime;
  }

  public Date getExecutableTime() {
    return executableTime;
  }

  public void setExecutableTime(Date executableTime) {
    this.executableTime = executableTime;
  }

  public Date getExecutingTime() {
    return executingTime;
  }

  public Date getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(Date finishTime) {
    this.finishTime = finishTime;
  }

  public void setExecutingTime(Date executingTime) {
    this.executingTime = executingTime;
  }

  public String getExecuteResult() {
    return executeResult;
  }

  public void setExecuteResult(String executeResult) {
    this.executeResult = executeResult;
  }

  public Date getNextTriggerTime() {
    return nextTriggerTime;
  }

  public void setNextTriggerTime(Date nextTriggerTime) {
    this.nextTriggerTime = nextTriggerTime;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sysId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TriggeredTaskInfo) {
      TriggeredTaskInfo o = (TriggeredTaskInfo)other;
      return sysId.equals(o.sysId);
    }
    return false;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
