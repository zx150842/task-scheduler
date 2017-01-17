package com.dts.rpc.netty;

import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcEnv;
import com.dts.rpc.RpcEnvFactory;
import com.dts.rpc.util.SerializerInstance;

/**
 * @author zhangxin
 */
public class NettyRpcEnvFactory implements RpcEnvFactory {
  @Override public RpcEnv create(DTSConf conf, String host, int port, boolean clientMode) {
    SerializerInstance serializerInstance = new SerializerInstance(conf);
    NettyRpcEnv nettyRpcEnv = new NettyRpcEnv(conf, serializerInstance, host);
    if (!clientMode) {
      try {
        nettyRpcEnv.startServer(port);
      } catch (Throwable e) {
        nettyRpcEnv.shutdown();
        throw e;
      }
    }
    return nettyRpcEnv;
  }
}
