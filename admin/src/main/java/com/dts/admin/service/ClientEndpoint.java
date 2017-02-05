package com.dts.admin.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.dts.admin.common.dto.MasterNodeDto;
import com.dts.admin.common.dto.WorkerNodeDto;
import com.dts.admin.common.vo.CronJob;
import com.dts.core.EndpointNames;
import com.dts.core.DTSConf;
import com.dts.core.DeployMessages;
import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.registration.RegisterClient;
import com.dts.core.registration.RegisterServiceName;
import com.dts.core.registration.RpcRegisterMessage;
import com.dts.core.registration.ZKNodeChangeListener;
import com.dts.core.rpc.RpcEndpoint;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.util.Tuple2;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhangxin
 */
public class ClientEndpoint extends RpcEndpoint {

  private static final String SYSTEM_NAME = "dtsClient";
  private static ClientEndpoint clientEndpoint;
  private final RegisterClient registerClient;
  private final DTSConf conf;
  private final RpcEnv rpcEnv;

  private Set<RpcEndpointRef> _masterRefs = Sets.newLinkedHashSet();
  private List<MasterNodeDto> _masters = Lists.newArrayList();
  private List<WorkerNodeDto> _workers = Lists.newArrayList();
  private List<String> _workerGroups = Lists.newArrayList();
  // workerGroup -> <taskName, param describe>
  private Map<String, Set<Tuple2<String, String>>> _workerGroupToTasks = Maps.newHashMap();

  private ClientEndpoint(RpcEnv rpcEnv, DTSConf conf) {
    super(rpcEnv);
    this.rpcEnv = rpcEnv;
    this.conf = conf;
    registerClient = new RegisterClient(conf, new MasterNodeChangeListener());
    registerClient.start();
  }

  public static ClientEndpoint endpoint() {
    if (clientEndpoint == null) {
      synchronized (ClientEndpoint.class) {
        if (clientEndpoint == null) {
          DTSConf conf = DTSConfUtil.readFile("dts.properties");
          int port = conf.getInt("dts.worker.port", 0);
          RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, AddressUtil.getLocalHost(), port, conf, true);
          clientEndpoint = new ClientEndpoint(rpcEnv, conf);
        }
      }
    }
    return clientEndpoint;
  }

  public boolean triggerJob(CronJob cronJob) {
    JobConf jobConf = new JobConf(cronJob.getJobId(), cronJob.getCronExpression(),
        cronJob.getWorkerGroup(), cronJob.getMaxRunTimeSec(), cronJob.getLastTriggerTime(), new TaskConf());
    for (RpcEndpointRef masterRef : _masterRefs) {
      masterRef.ask(new DeployMessages.ManualTriggerJob(jobConf));
    }
    return true;
  }

  public List<WorkerNodeDto> getWorkers() {
    return _workers;
  }

  public List<String> getWorkerGroups() {
    return _workerGroups;
  }

  public Set<Tuple2<String, String>> getTasks(String workerGroup) {
    return _workerGroupToTasks.get(workerGroup);
  }

  public List<MasterNodeDto> getMasters() {
    return _masters;
  }

  class MasterNodeChangeListener implements ZKNodeChangeListener {

    @Override
    public void onChange(String serviceName, List<RpcRegisterMessage> messages) {
      if (serviceName.equals(RegisterServiceName.MASTER)) {
        Set<RpcEndpointRef> masterRefs = Sets.newLinkedHashSet();
        List<MasterNodeDto> masters = Lists.newArrayList();
        for (RpcRegisterMessage message : messages) {
          RpcEndpointAddress address = new RpcEndpointAddress(message.address, EndpointNames.MASTER_ENDPOINT);
          RpcEndpointRef masterRef = new NettyRpcEndpointRef(conf, address, (NettyRpcEnv) rpcEnv);
          masterRefs.add(masterRef);
          MasterNodeDto node = new MasterNodeDto(address.getRpcAddress().host, address.getRpcAddress().port);
          masters.add(node);
        }
        _masterRefs = masterRefs;
        _masters = masters;
      } else if (serviceName.equals(RegisterServiceName.WORKER)) {
        List<WorkerNodeDto> workers = Lists.newArrayList();
        List<String> workerGroups = Lists.newArrayList();
        Map<String, Set<Tuple2<String, String>>> workerGroupToTasks = Maps.newHashMap();
        for (RpcRegisterMessage message : messages) {
          String workerGroup = message.detail.getWorkerGroup();
          WorkerNodeDto node = new WorkerNodeDto(message.address.host,
              message.address.port, message.detail.getWorkerId(), workerGroup,
              message.detail.getThreadNum(), message.detail.getTaskMethods());
          workers.add(node);
          workerGroups.add(workerGroup);
          if (!workerGroupToTasks.containsKey(workerGroup)) {
            Set<Tuple2<String, String>> tasks = Sets.newHashSet();
            for (String taskName : message.detail.getTaskMethods().keySet()) {
              tasks.add(new Tuple2<>(taskName, message.detail.getTaskMethods().get(taskName)));
            }
            workerGroupToTasks.put(workerGroup, tasks);
          }
        }
        _workers = workers;
        _workerGroups = workerGroups;
        _workerGroupToTasks = workerGroupToTasks;
      } else {
        throw new IllegalArgumentException("serviceName [" + StringUtils.trimToEmpty(serviceName)
            + "] must be either 'MASTER' or 'WORKER'");
      }
    }

    @Override
    public List<String> getListeningServiceNames() {
      return Lists.newArrayList(RegisterServiceName.MASTER, RegisterServiceName.WORKER);
    }
  }
}
