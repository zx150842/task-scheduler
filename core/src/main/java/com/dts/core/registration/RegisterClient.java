package com.dts.core.registration;

import com.dts.core.util.CuratorUtil;
import com.dts.core.DTSConf;
import com.dts.core.exception.DTSException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 注册中心，当前为zookeeper实现。主要用来实现master和worker的注册和发现
 *
 * @author zhangxin
 */
public class RegisterClient {
  private ServiceDiscovery<WorkerNodeDetail> serviceDiscovery;
  private ZKNodeChangeListener listener;
  private Map<String, ServiceCache<WorkerNodeDetail>> serviceCaches = Maps.newHashMap();
  private Object serviceCacheLock = new Object();

  public RegisterClient(DTSConf conf, ZKNodeChangeListener listener) {
    CuratorFramework zk = CuratorUtil.newClient(conf);
    String basePath = conf.get("dts.zookeeper.dir", "/dts") + "/register";
    JsonInstanceSerializer<WorkerNodeDetail> serializer = new JsonInstanceSerializer<>(WorkerNodeDetail.class);
    serviceDiscovery =
      ServiceDiscoveryBuilder.builder(WorkerNodeDetail.class).client(zk).basePath(basePath).serializer(serializer).build();
    this.listener = listener;

    List<String> listeningServiceNames = listener.getListeningServiceNames();
    for (String serviceName : listeningServiceNames) {
      registerServiceCache(serviceName);
    }
  }

  public void start() {
    try {
      serviceDiscovery.start();
      for (String serviceName : serviceCaches.keySet()) {
        serviceCaches.get(serviceName).start();
      }
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public void registerService(ServiceInstance serviceInstance) {
    try {
      serviceDiscovery.registerService(serviceInstance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public void unregisterService(ServiceInstance serviceInstance) {
    try {
      serviceDiscovery.unregisterService(serviceInstance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public void updateService(ServiceInstance serviceInstance) {
    try {
      serviceDiscovery.updateService(serviceInstance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public Set<String> getAllServiceName() {
    try {
      return Sets.newHashSet(serviceDiscovery.queryForNames());
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public List<RpcRegisterMessage> getByServiceName(String serviceName) {
    List<ServiceInstance<WorkerNodeDetail>> instances = serviceCaches.get(serviceName).getInstances();
    List<RpcRegisterMessage> messages = Lists.newArrayList();
    for (ServiceInstance<WorkerNodeDetail> instance : instances) {
      messages.add(new RpcRegisterMessage(instance));
    }
    return messages;
  }

  public void close() {
    try {
      serviceDiscovery.close();
      synchronized (serviceCacheLock) {
        for (String serviceName : serviceCaches.keySet()) {
          serviceCaches.get(serviceName).close();
        }
      }
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  private void registerServiceCache(String serviceName) {
    synchronized (serviceCacheLock) {
      if (!serviceCaches.containsKey(serviceName)) {
        ServiceCache serviceCache = serviceDiscovery.serviceCacheBuilder().name(serviceName).build();
        serviceCache.addListener(new ServiceCacheListener() {
          @Override
          public void cacheChanged() {
            listener.onChange(serviceName, getByServiceName(serviceName));
          }

          @Override
          public void stateChanged(CuratorFramework client, ConnectionState newState) {
            listener.onChange(serviceName, getByServiceName(serviceName));
          }
        });
        serviceCaches.put(serviceName, serviceCache);
      }
    }
  }
}
