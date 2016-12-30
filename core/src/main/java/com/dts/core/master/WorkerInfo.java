package com.dts.core.master;

import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.RpcEndpointRef;

/**
 * @author zhangxin
 */
public class WorkerInfo {

  public final String id;
  public final String host;
  public final int port;
  public final int cores;
  public final int memory;
  public final RpcEndpointRef endpoint;
  public final String groupId;

  private WorkerState state;
  private int coresUsed;
  private int memoryUsed;
  private long lastHeartbeat;

  public WorkerInfo(String id, String host, int port, int cores, int memory, RpcEndpointRef endpoint, String groupId) {
    this.id = id;
    this.host = host;
    this.port = port;
    this.cores = cores;
    this.memory = memory;
    this.endpoint = endpoint;
    this.groupId = groupId;
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

  public int getMemoryUsed() {
    return memoryUsed;
  }

  public void setMemoryUsed(int memoryUsed) {
    this.memoryUsed = memoryUsed;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }
}
