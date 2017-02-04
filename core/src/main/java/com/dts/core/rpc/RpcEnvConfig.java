package com.dts.core.rpc;

import com.dts.core.DTSConf;

/**
 * @author zhangxin
 */
public class RpcEnvConfig {
  public final DTSConf conf;
  public final String name;
  public final String host;
  public final int port;
  public final boolean clientMode;

  public RpcEnvConfig(DTSConf conf, String name, String host, int port, boolean clientMode) {
    this.conf = conf;
    this.name = name;
    this.host = host;
    this.port = port;
    this.clientMode = clientMode;
  }
}
