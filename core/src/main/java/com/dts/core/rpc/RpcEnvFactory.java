package com.dts.core.rpc;

/**
 * @author zhangxin
 */
public interface RpcEnvFactory {

  RpcEnv create(RpcEnvConfig rpcEnvConfig);
}
