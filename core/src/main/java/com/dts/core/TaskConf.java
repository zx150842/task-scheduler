package com.dts.core;

import java.util.Date;
import java.util.LinkedHashMap;

/**
 * @author zhangxin
 */
public class TaskConf {
  // 用户指定的task id
  private String taskId;
  // cron表达式
  private String cronExpression;
  // task提交到的worker组
  private String workerGroup;
  // task所属的组
  private String taskGroup;
  // task的传入参数
  private LinkedHashMap<String, String> params;
  // task创建时间
  private Long ctime;
  // task更新时间
  private Long utime;
  // task状态
  private int status;
  // task被触发的时间
  private Date triggerTime;
  // 任务允许运行的最大时间（ms）
  private long maxRunTimeMs = -1;
  // 下一个task id
  private String nextTaskId;

  public TaskConf(String taskId, String cronExpression, String workerGroup, String taskGroup) {
    this(taskId, cronExpression, workerGroup, taskGroup, null);
  }

  public TaskConf(String taskId, String cronExpression, String workerGroup, String taskGroup,
    LinkedHashMap<String, String> params) {
    this.taskId = taskId;
    this.cronExpression = cronExpression;
    this.workerGroup = workerGroup;
    this.taskGroup = taskGroup;
    this.params = params;
    this.status = 1;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
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

  public String getTaskGroup() {
    return taskGroup;
  }

  public void setTaskGroup(String taskGroup) {
    this.taskGroup = taskGroup;
  }

  public LinkedHashMap<String, String> getParams() {
    return params;
  }

  public void setParams(LinkedHashMap<String, String> params) {
    this.params = params;
  }

  public Long getCtime() {
    return ctime;
  }

  public void setCtime(Long ctime) {
    this.ctime = ctime;
  }

  public Long getUtime() {
    return utime;
  }

  public void setUtime(Long utime) {
    this.utime = utime;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public Date getTriggerTime() {
    return triggerTime;
  }

  public void setTriggerTime(Date triggerTime) {
    this.triggerTime = triggerTime;
  }

  public long getMaxRunTimeMs() {
    return maxRunTimeMs;
  }

  public void setMaxRunTimeMs(long maxRunTimeMs) {
    this.maxRunTimeMs = maxRunTimeMs;
  }

  public String getNextTaskId() {
    return nextTaskId;
  }

  public void setNextTaskId(String nextTaskId) {
    this.nextTaskId = nextTaskId;
  }
}
