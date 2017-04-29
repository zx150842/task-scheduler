package com.dts.admin.common.dto;

import com.dts.core.rpc.RpcEndpointRef;

/**
 * @author zhangxin
 */
public class MasterNodeDto {
  private String host;
  private int port;
  private boolean isLeader;
  private RpcEndpointRef masterRef;

  public MasterNodeDto(String host, int port, boolean isLeader, RpcEndpointRef masterRef) {
    this.host = host;
    this.port = port;
    this.isLeader = isLeader;
    this.masterRef = masterRef;
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

  public boolean isLeader() {
    return isLeader;
  }

  public void setLeader(boolean leader) {
    isLeader = leader;
  }

  public RpcEndpointRef getMasterRef() {
    return masterRef;
  }

  public void setMasterRef(RpcEndpointRef masterRef) {
    this.masterRef = masterRef;
  }
}
