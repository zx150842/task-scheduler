package com.dts.core.registration;

import org.codehaus.jackson.map.annotate.JsonRootName;

import java.util.Map;

/**
 * worker节点的注册信息
 *
 * @author zhangxin
 */
@JsonRootName("details")
public class WorkerNodeDetail {
  private String workerId;
  private String workerGroup;
  private int threadNum;
  // taskName -> method desc
  private Map<String, String> taskMethods;

  public WorkerNodeDetail() {}

  public WorkerNodeDetail(String workerId, String workerGroup, int threadNum, Map<String, String> taskMethods) {
    this.workerId = workerId;
    this.workerGroup = workerGroup;
    this.threadNum = threadNum;
    this.taskMethods = taskMethods;
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

  public Map<String, String> getTaskMethods() {
    return taskMethods;
  }

  public void setTaskMethods(Map<String, String> taskMethods) {
    this.taskMethods = taskMethods;
  }

  public int getThreadNum() {
    return threadNum;
  }

  public void setThreadNum(int threadNum) {
    this.threadNum = threadNum;
  }
}
