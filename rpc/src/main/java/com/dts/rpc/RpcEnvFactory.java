package com.dts.rpc;

/**
 * @author zhangxin
 */
public interface RpcEnvFactory {

  RpcEnv create(RpcEnvConfig rpcEnvConfig);
}
