package com.dts.admin.common.dto;

/**
 * @author zhangxin
 */
public class MasterNodeDto {
  private String host;
  private int port;
  private boolean isLeader;

  public MasterNodeDto(String host, int port) {
    this.host = host;
    this.port = port;
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
}
