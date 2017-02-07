package com.dts.admin.service;

import org.junit.Test;

/**
 * @author zhangxin
 */
public class ClientEndpointTest {
  @Test
  public void test() {
    ClientEndpoint clientEndpoint = ClientEndpoint.endpoint();
    clientEndpoint.refresh();
  }
}
