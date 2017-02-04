package com.dts.core.rpc.netty;

import com.dts.core.rpc.RpcEndpointAddress;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author zhangxin
 */
public class NettyRpcAddressTest {

  @Test
  public void testToString() {
    RpcEndpointAddress addr = new RpcEndpointAddress("localhost", 12345, "test");
    assertEquals("test@localhost:12345", addr.toString());
  }

  @Test
  public void testToStringClient() {
    RpcEndpointAddress addr = new RpcEndpointAddress(null, "test");
    assertEquals("test", addr.toString());
  }
}
