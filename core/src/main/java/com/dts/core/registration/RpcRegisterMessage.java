package com.dts.core.registration;

import com.dts.rpc.RpcAddress;
import org.apache.curator.x.discovery.ServiceInstance;

/**
 * @author zhangxin
 */
public class RpcRegisterMessage {
  public final RpcAddress address;
  public final WorkerNodeDetail detail;

  public RpcRegisterMessage(ServiceInstance<WorkerNodeDetail> instance) {
    address = new RpcAddress(instance.getAddress(), instance.getPort());
    detail = instance.getPayload();
  }
}
