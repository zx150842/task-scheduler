package com.dts.admin.common.dto;

/**
 * @author zhangxin
 */
public class TaskDto {
  private String taskId;
  private String taskName;
  private String params;

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
}
