package com.dts.scheduler;

import com.dts.core.rpc.RpcEndpointRef;

import java.io.Serializable;

/**
 * worker节点信息
 *
 * @author zhangxin
 */
public class WorkerInfo implements Serializable {

  public final String id;
  public final String host;
  public final int port;
  public final RpcEndpointRef endpoint;
  public final String groupId;
  public final boolean isSeed;

  transient private WorkerState state;
  transient private int coresUsed;
  transient private long memoryUsed;
  transient private long lastHeartbeat;

  public WorkerInfo(String id, String groupId, boolean isSeed, RpcEndpointRef endpoint) {
    this.id = id;
    this.host = endpoint.address().host;
    this.port = endpoint.address().port;
    this.endpoint = endpoint;
    this.groupId = groupId;
    this.isSeed = isSeed;
    init();
  }

  private void init() {
    state = WorkerState.ALIVE;
    coresUsed = 0;
    memoryUsed = 0;
    lastHeartbeat = System.currentTimeMillis();
  }

  public WorkerState getState() {
    return this.state;
  }

  public void setState(WorkerState state) {
    this.state = state;
  }

  public boolean isAlive() {
    return this.state == WorkerState.ALIVE;
  }

  public int getCoresUsed() {
    return coresUsed;
  }

  public void setCoresUsed(int coresUsed) {
    this.coresUsed = coresUsed;
  }

  public long getMemoryUsed() {
    return memoryUsed;
  }

  public void setMemoryUsed(long memoryUsed) {
    this.memoryUsed = memoryUsed;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }
}
