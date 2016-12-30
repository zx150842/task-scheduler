package com.dts.core.master;

import com.dts.rpc.RpcEndpointRef;

/**
 * @author zhangxin
 */
public class ClientInfo {

  public final String id;
  public final String host;
  public final int port;
  public final RpcEndpointRef endpoint;

  private long lastHeartbeat;
  private ClientState state;

  public ClientInfo(String id, String host, int port, RpcEndpointRef endpoint) {
    this.id = id;
    this.host = host;
    this.port = port;
    this.endpoint = endpoint;
  }

  public String getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public RpcEndpointRef getEndpoint() {
    return endpoint;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  public ClientState getState() {
    return state;
  }

  public void setState(ClientState state) {
    this.state = state;
  }
}
