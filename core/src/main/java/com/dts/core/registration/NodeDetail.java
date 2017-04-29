package com.dts.core.registration;

import org.codehaus.jackson.map.annotate.JsonRootName;

import java.io.Serializable;
import java.util.Map;

/**
 * worker节点的注册信息
 *
 * @author zhangxin
 */
@JsonRootName("details")
public class NodeDetail implements Serializable {
  private static final long serialVersionUID = -8387129741672575562L;
  private String workerId;
  private String workerGroup;
  private int threadNum;
  // taskName -> method desc
  private Map<String, String> taskMethods;
  private boolean isSeed;

  public NodeDetail() {}

  public NodeDetail(boolean isSeed) {
    this.isSeed = isSeed;
  }

  public NodeDetail(
    String workerId,
    String workerGroup,
    int threadNum,
    Map<String, String> taskMethods,
    boolean isSeed) {
    this.workerId = workerId;
    this.workerGroup = workerGroup;
    this.threadNum = threadNum;
    this.taskMethods = taskMethods;
    this.isSeed = isSeed;
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

  public boolean isSeed() {
    return isSeed;
  }

  public void setSeed(boolean seed) {
    isSeed = seed;
  }
}
