package com.dts.core.registration;

import com.dts.core.util.CuratorUtil;
import com.dts.rpc.DTSConf;
import com.dts.rpc.exception.DTSException;
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
 * @author zhangxin
 */
public class RegisterClient {
  private ServiceDiscovery<WorkerNodeDetail> serviceDiscovery;
  private ZKNodeChangeListener listener;
  private Map<String, ServiceCache<WorkerNodeDetail>> serviceCaches = Maps.newHashMap();
  private Object serviceCahceLock = new Object();

  public RegisterClient(DTSConf conf, ZKNodeChangeListener listener) {
    try {
      CuratorFramework zk = CuratorUtil.newClient(conf);
      String basePath = conf.get("dts.zookeeper.dir", "/dts") + "/register";
      JsonInstanceSerializer<WorkerNodeDetail> serializer = new JsonInstanceSerializer<>(WorkerNodeDetail.class);
      serviceDiscovery =
        ServiceDiscoveryBuilder.builder(WorkerNodeDetail.class).client(zk).basePath(basePath).serializer(serializer).build();
      this.listener = listener;

      serviceDiscovery.start();
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public void registerService(ServiceInstance<WorkerNodeDetail> serviceInstance) {
    try {
      String serviceName = serviceInstance.getName();
      if (!serviceCaches.containsKey(serviceName)) {
        registerServiceCache(serviceName);
      }
      serviceDiscovery.registerService(serviceInstance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public void unregisterService(ServiceInstance<WorkerNodeDetail> serviceInstance) {
    try {
      serviceDiscovery.unregisterService(serviceInstance);
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  public void updateService(ServiceInstance<WorkerNodeDetail> serviceInstance) {
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
      synchronized (serviceCahceLock) {
        for (String serviceName : serviceCaches.keySet()) {
          serviceCaches.get(serviceName).close();
        }
      }
    } catch (Exception e) {
      throw new DTSException(e);
    }
  }

  private void registerServiceCache(String serviceName) throws Exception {
    synchronized (serviceCahceLock) {
      if (!serviceCaches.containsKey(serviceName)) {
        ServiceCache serviceCache = serviceDiscovery.serviceCacheBuilder().name(serviceName).build();
        serviceCache.addListener(new ServiceCacheListener() {
          @Override public void cacheChanged() {
            listener.onChange(serviceName, getByServiceName(serviceName));
          }
          @Override public void stateChanged(CuratorFramework client, ConnectionState newState) {
            listener.onChange(serviceName, getByServiceName(serviceName));
          }
        });
        serviceCaches.put(serviceName, serviceCache);
        serviceCache.start();
      }
    }
  }
}
