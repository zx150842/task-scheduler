package com.dts.core.registration;

import com.dts.core.rpc.RpcAddress;
import org.apache.curator.x.discovery.ServiceInstance;

import java.io.Serializable;

/**
 * @author zhangxin
 */
public class RpcRegisterMessage implements Serializable {
  private static final long serialVersionUID = -3028609199397394895L;
  public RpcAddress address;
  public NodeDetail detail;

  public RpcRegisterMessage() {}

  public RpcRegisterMessage(ServiceInstance<NodeDetail> instance) {
    address = new RpcAddress(instance.getAddress(), instance.getPort());
    detail = instance.getPayload();
  }
}
