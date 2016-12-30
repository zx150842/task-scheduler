package com.dts.core;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author zhangxin
 */
public class TaskGroup {
  // task group的系统内部id
  private String id;
  // task group包含的task列表
  private List<TaskInfo> tasks;
  // cron表达式
  private String cronExpression;
  // task提交到的worker组
  private String workerGroup;
  // task group id
  private String taskGroupId;
  // task group创建时间
  private Long ctime;
  // task group更新时间
  private Long utime;
  // task group状态
  private int status;
  // task被触发的时间
  private Date triggerTime;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<TaskInfo> getTasks() {
    return tasks;
  }

  public void setTasks(List<TaskInfo> tasks) {
    this.tasks = tasks;
  }

  public String getTaskGroupId() {
    return taskGroupId;
  }

  public void setTaskGroupId(String taskGroupId) {
    this.taskGroupId = taskGroupId;
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
}
