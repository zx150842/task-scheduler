package com.dts.core;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class TaskConf implements Serializable {
  private static final long serialVersionUID = -4042396408823866502L;
  // task的唯一id
  private String taskId;
  // task名称，用于找到worker上的实际执行方法
  private String taskName;
  // param type and value json
  private String params;

  public TaskConf() {}

  public TaskConf(String taskId, String taskName, String params) {
    this.taskId = taskId;
    this.taskName = taskName;
    this.params = params;
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
}
