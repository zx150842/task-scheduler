package com.dts.rpc;

/**
 * @author zhangxin
 */
public interface RpcEnvFactory {

  RpcEnv create(DTSConf conf, String host, int port, boolean clientMode);
}
