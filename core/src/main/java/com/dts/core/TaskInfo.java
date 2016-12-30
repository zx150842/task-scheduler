package com.dts.core;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author zhangxin
 */
public class TaskInfo implements Serializable {
  // task的系统内部id
  private String id;
  // 用户指定的task id
  private String taskId;
  // cron表达式
  private String cronExpression;
  // task提交到的worker组
  private String workerGroup;
  // task所属的组
  private String groupId;
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
  //
  private String nextTaskId;
  // 当前task完成后，接下来要执行的task的系统内部id（适用于一组task）
  private String nextId;
  // 是否是手动触发
  private boolean manualTrigger;

  public TaskInfo(String taskId, String cronExpression, String workerGroup, String taskGroup) {
    this(taskId, cronExpression, workerGroup, taskGroup, null);
  }

  public TaskInfo(String taskId, String cronExpression, String workerGroup, String groupId,
      LinkedHashMap<String, String> params) {
    this.taskId = taskId;
    this.cronExpression = cronExpression;
    this.workerGroup = workerGroup;
    this.groupId = groupId;
    this.params = params;
    this.status = 1;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
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

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
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

  public String getNextId() {
    return nextId;
  }

  public void setNextId(String nextId) {
    this.nextId = nextId;
  }

  public String getNextTaskId() {
    return nextTaskId;
  }

  public void setNextTaskId(String nextTaskId) {
    this.nextTaskId = nextTaskId;
  }

  public boolean isManualTrigger() {
    return manualTrigger;
  }

  public void setManualTrigger(boolean manualTrigger) {
    this.manualTrigger = manualTrigger;
  }
}
