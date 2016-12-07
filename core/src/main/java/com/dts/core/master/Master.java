package com.dts.core.master;

import com.dts.rpc.DTSConf;
import com.dts.rpc.RpcAddress;
import com.dts.rpc.RpcCallContext;
import com.dts.rpc.RpcEndpoint;
import com.dts.rpc.netty.NettyRpcEnv;

/**
 * @author zhangxin
 */
public class Master extends RpcEndpoint {
  private final RpcAddress address;
  private final DTSConf conf;

  private final long WORKER_TIMEOUT_MS;

  public Master(NettyRpcEnv rpcEnv, RpcAddress address, DTSConf conf) {
    super(rpcEnv);
    this.address = address;
    this.conf = conf;
  }

  @Override
  public void onStart() {

  }

  @Override
  public void onStop() {

  }

  public void electedLeader() {
    self().send();
  }

  @Override
  public void receive(Object o) {

  }

  @Override
  public void receiveAndReply(RpcCallContext context) {

  }
}
