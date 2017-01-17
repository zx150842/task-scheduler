package com.dts.core;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class TaskInfo implements Serializable {
  public final TaskConf taskConf;
  // task的系统内部id
  private String id;
  // 当前task完成后，接下来要执行的task的系统内部id（适用于一组task）
  private String nextId;
  // 是否是手动触发
  private boolean manualTrigger;

  private int status;
  // task被分配运行的workerId
  private String workerId;
  // task被分配运行的线程名
  private String threadName;

  public TaskInfo(TaskConf taskConf) {
    this.taskConf = taskConf;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getNextId() {
    return nextId;
  }

  public void setNextId(String nextId) {
    this.nextId = nextId;
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

  public String getThreadName() {
    return threadName;
  }

  public void setThreadName(String threadName) {
    this.threadName = threadName;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }
}
