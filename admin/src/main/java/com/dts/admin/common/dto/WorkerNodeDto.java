package com.dts.admin.common.dto;

import java.util.Map;

/**
 * @author zhangxin
 */
public class WorkerNodeDto {
  private String host;
  private int port;
  private String workerId;
  private String workerGroup;
  private int threadNum;
  // taskName -> method desc
  private Map<String, String> taskMethods;

  public WorkerNodeDto(String host, int port, String workerId, String workerGroup,
                       int threadNum, Map<String, String> taskMethods) {
    this.host = host;
    this.port = port;
    this.workerId = workerId;
    this.workerGroup = workerGroup;
    this.threadNum = threadNum;
    this.taskMethods = taskMethods;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getWorkerId() {
    return workerId;
  }

  public void setWorkerId(String workerId) {
    this.workerId = workerId;
  }

  public String getWorkerGroup() {
    return workerGroup;
  }

  public void setWorkerGroup(String workerGroup) {
    this.workerGroup = workerGroup;
  }

  public int getThreadNum() {
    return threadNum;
  }

  public void setThreadNum(int threadNum) {
    this.threadNum = threadNum;
  }

  public Map<String, String> getTaskMethods() {
    return taskMethods;
  }

  public void setTaskMethods(Map<String, String> taskMethods) {
    this.taskMethods = taskMethods;
  }
}
