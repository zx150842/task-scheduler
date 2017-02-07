package com.dts.core;

import com.dts.core.registration.*;
import com.google.common.collect.Lists;

import org.apache.curator.test.TestingServer;
import org.apache.curator.x.discovery.ServiceInstance;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class RegisterClientTest {

  @Test
  public void testRegister() throws Exception {
    DTSConf conf = new DTSConf(false);
    TestingServer zkTestServer = new TestingServer(12345);
    try {
      conf.set("dts.master.zookeeper.url", zkTestServer.getConnectString());
      RegisterClient registerClient = new RegisterClient(conf, new ZKNodeChangeListener() {
        @Override public void onChange(String serviceName, List<RpcRegisterMessage> messages) {
          for (RpcRegisterMessage message : messages) {
            System.out.println("cache: " + message.address.hostPort);
          }
        }

        @Override
        public List<String> getListeningServiceNames() {
          return Lists.newArrayList(RegisterServiceName.WORKER);
        }
      });
      registerClient.start();
      ServiceInstance<WorkerNodeDetail> instance1 =
        ServiceInstance.<WorkerNodeDetail>builder().name(RegisterServiceName.WORKER).port(12346).address("127.0.0.1").build();
      ServiceInstance<WorkerNodeDetail> instance2 =
        ServiceInstance.<WorkerNodeDetail>builder().name("service1").port(12346).address("127.0.0.1").build();
      ServiceInstance<WorkerNodeDetail> instance3 =
        ServiceInstance.<WorkerNodeDetail>builder().name("service1").port(12348).address("127.0.0.1").build();
      registerClient.registerService(instance1);
      registerClient.registerService(instance2);
//      registerClient.unregisterService(instance1);
//      registerClient.updateService(instance2);
      TimeUnit.SECONDS.sleep(1);
//      List<RpcRegisterMessage> list = registerClient.getAllInstance();
//      for (RpcRegisterMessage m : list) {
//        System.out.println("get: " + m);
//      }
    } finally {
      zkTestServer.stop();
    }
  }
}
