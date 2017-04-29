package com.dts.admin.service;

import com.dts.core.registration.*;
import com.google.common.base.Throwables;
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
import com.dts.core.rpc.RpcEndpoint;
import com.dts.core.rpc.RpcEndpointAddress;
import com.dts.core.rpc.RpcEndpointRef;
import com.dts.core.rpc.RpcEnv;
import com.dts.core.rpc.netty.NettyRpcEndpointRef;
import com.dts.core.rpc.netty.NettyRpcEnv;
import com.dts.core.util.AddressUtil;
import com.dts.core.util.DTSConfUtil;
import com.dts.core.util.Tuple2;

import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.dts.core.DeployMessages.*;

/**
 * @author zhangxin
 */
public class ClientEndpoint extends RpcEndpoint {
  private final Logger logger = LoggerFactory.getLogger(ClientEndpoint.class);

  private static final String SYSTEM_NAME = "dtsClient";
  private static ClientEndpoint clientEndpoint;
  private final RegisterClient registerClient;
  private final DTSConf conf;
  private final RpcEnv rpcEnv;

  private List<MasterNodeDto> _masters = Lists.newArrayList();
  private List<WorkerNodeDto> _onlineWorkers = Lists.newArrayList();
  private List<WorkerNodeDto> _seedWorkers = Lists.newArrayList();
  private Set<String> _workerGroups = Sets.newHashSet();
  // workerGroup -> <taskName, param describe>
  private Map<String, Set<Tuple2<String, String>>> _workerGroupToTasks = Maps.newHashMap();

  private final int MASTER_TIMEOUT_MS;
  private final Gson GSON = new Gson();

  private ClientEndpoint(RpcEnv rpcEnv, DTSConf conf) {
    super(rpcEnv);
    this.rpcEnv = rpcEnv;
    this.conf = conf;
    this.MASTER_TIMEOUT_MS = conf.getInt("dts.master.timeoutMs", 5 * 1000);
    this.registerClient = new RegisterClient(conf, new MasterWorkerNodeChangeListener());
  }

  @Override
  public void onStart() {
    registerClient.start();
    refreshMasters();
    refreshWorkers();
  }

  public static ClientEndpoint endpoint() {
    if (clientEndpoint == null) {
      synchronized (ClientEndpoint.class) {
        if (clientEndpoint == null) {
          DTSConf conf = DTSConfUtil.readFile("dts.properties");
          int port = conf.getInt("dts.worker.port", 0);
          RpcEnv rpcEnv = RpcEnv.create(SYSTEM_NAME, AddressUtil.getLocalHost(), port, conf, true);
          clientEndpoint = new ClientEndpoint(rpcEnv, conf);
          rpcEnv.setupEndpoint(EndpointNames.CLIENT_ENDPOINT, clientEndpoint);
        }
      }
    }
    return clientEndpoint;
  }

  public String triggerJob(CronJob cronJob, boolean runOnSeed) {
    List<TaskConf> tasks = GSON.fromJson(cronJob.getTasks(), new TypeToken<List<TaskConf>>(){}.getType());
    if (tasks == null || tasks.size() != 1) {
      return null;
    }
    JobConf jobConf = new JobConf(cronJob.getJobId(), cronJob.getCronExpression(),
        cronJob.getWorkerGroup(), cronJob.getMaxRunTimeSec(), cronJob.getLastTriggerTime(), tasks.get(0));
    Optional<MasterNodeDto> master = _masters.stream().filter(m -> m.isLeader()).findFirst();
    if (master.isPresent()) {
      Future<ManualTriggeredJob> future = master.get().getMasterRef().ask(new ManualTriggerJob(jobConf, runOnSeed));
      try {
        ManualTriggeredJob ret = future.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return ret.id;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        logger.error(Throwables.getStackTraceAsString(e));
      }
    }
    return null;
  }

  public boolean refreshJobs() throws Exception {
    SettableFuture<Boolean> success = SettableFuture.create();
    _masters.stream().filter(m -> m.isLeader()).forEach(m -> {
      Future<RefreshedJobs> future = m.getMasterRef().ask(new RefreshJobs());
      try {
        RefreshedJobs msg = future.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        success.set(msg.code == SUCCESS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        success.setException(e);
      }
    });
    return success.get();
  }

  public List<WorkerNodeDto> getOnlineWorkers() {
    return _onlineWorkers;
  }

  public List<WorkerNodeDto> getSeedWorkers() {
    return _seedWorkers;
  }

  public List<String> getWorkerGroups() {
    return Lists.newArrayList(_workerGroups);
  }

  public Set<Tuple2<String, String>> getTasks(String workerGroup) {
    return _workerGroupToTasks.get(workerGroup);
  }

  public List<MasterNodeDto> getMasters() {
    return _masters;
  }

  /**
   * 刷新leader scheduler内存中的workers
   *
   * @return
   * @throws Exception
   */
  public boolean refreshMasterWorkers() throws Exception {
    SettableFuture<Boolean> success = SettableFuture.create();
    _masters.stream().filter(m -> m.isLeader()).forEach(m -> {
      Future<ReplyWorkers> future = m.getMasterRef().ask(new RefreshWorkers());
      try {
        ReplyWorkers msg = future.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        refreshClientWorkers(msg.workers);
        success.set(true);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        success.setException(e);
      }
    });
    return success.get();
  }

  public boolean refreshMasters() {
    List<RpcRegisterMessage> masterMsgs = registerClient.getByServiceName(RegisterServiceName.MASTER);
    refreshMasters(masterMsgs);
    return true;
  }

  void refreshMasters(List<RpcRegisterMessage> messages) {
    List<MasterNodeDto> masters = Lists.newArrayList();
    for (RpcRegisterMessage message : messages) {
      try {
        RpcEndpointAddress address = new RpcEndpointAddress(message.address, EndpointNames.MASTER_ENDPOINT);
        RpcEndpointRef masterRef = new NettyRpcEndpointRef(conf, address, (NettyRpcEnv) rpcEnv);
        Boolean isLeader = masterRef.askWithRetry(new DeployMessages.AskLeader());
        if (isLeader == null) {
          isLeader = false;
        }
        MasterNodeDto node = new MasterNodeDto(address.getRpcAddress().host, address.getRpcAddress().port, isLeader, masterRef);
        masters.add(node);
      } catch (Exception e) {
        logger.error(Throwables.getStackTraceAsString(e));
        continue;
      }
    }
    _masters = masters;
  }

  void refreshWorkers() {
    List<RpcRegisterMessage> messages = registerClient.getByServiceName(RegisterServiceName.WORKER);
    refreshWorkers(messages);
  }

  /**
   * 如果ZooKeeper中worker节点发生变化，则admin首先从scheduler获取当前的workers，
   * 如果scheduler的worker列表与ZooKeeper中最新的worker列表不相同，则先刷新scheduler
   * 的worker列表，然后再刷新admin的worker列表
   *
   * @param messages
   */
  void refreshWorkers(List<RpcRegisterMessage> messages) {
    final Set<String> zkWorkerIds = messages.stream()
      .map(msg -> msg.detail.getWorkerId())
      .collect(Collectors.toSet());
    SettableFuture<Boolean> needToRefreshMasterWorks = SettableFuture.create();
    _masters.stream().filter(m -> m.isLeader()).forEach(m -> {
      Future<ReplyWorkers> future = m.getMasterRef().ask(new AskWorkers());
      try {
        ReplyWorkers msg = future.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        List<RpcRegisterMessage> masterWorkers = msg.workers;
        if (masterWorkers.size() != zkWorkerIds.size()) {
          needToRefreshMasterWorks.set(true);
        } else {
          needToRefreshMasterWorks.set(masterWorkers.stream()
            .anyMatch(worker -> !zkWorkerIds.contains(worker.detail.getWorkerId())));
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        logger.error(Throwables.getStackTraceAsString(e));
        needToRefreshMasterWorks.setException(e);
      }
    });
    try {
      boolean needToRefresh = needToRefreshMasterWorks.get(MASTER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (needToRefresh) {
        boolean success = refreshMasterWorkers();
        if (!success) {
          logger.error("Fail to refresh master workers");
        }
      } else {
        refreshClientWorkers(messages);
      }
    } catch (Exception e) {
      logger.error(Throwables.getStackTraceAsString(e));
    }
  }

  void refreshClientWorkers(List<RpcRegisterMessage> messages) {
    List<WorkerNodeDto> onlineWorkers = Lists.newArrayList();
    List<WorkerNodeDto> seedWorkers = Lists.newArrayList();
    Set<String> workerGroups = Sets.newHashSet();
    Map<String, Set<Tuple2<String, String>>> workerGroupToTasks = Maps.newHashMap();
    for (RpcRegisterMessage message : messages) {
      String workerGroup = message.detail.getWorkerGroup();
      WorkerNodeDto node = new WorkerNodeDto(message.address.host,
        message.address.port, message.detail.getWorkerId(), workerGroup,
        message.detail.getThreadNum(), message.detail.getTaskMethods());
      if (message.detail.isSeed()) {
        seedWorkers.add(node);
      } else {
        onlineWorkers.add(node);
      }
      workerGroups.add(workerGroup);
      if (!workerGroupToTasks.containsKey(workerGroup)) {
        Set<Tuple2<String, String>> tasks = Sets.newHashSet();
        for (String taskName : message.detail.getTaskMethods().keySet()) {
          tasks.add(new Tuple2<>(taskName, message.detail.getTaskMethods().get(taskName)));
        }
        workerGroupToTasks.put(workerGroup, tasks);
      }
    }
    _onlineWorkers = onlineWorkers;
    _seedWorkers = seedWorkers;
    _workerGroups = workerGroups;
    _workerGroupToTasks = workerGroupToTasks;
  }

  class MasterWorkerNodeChangeListener implements ZKNodeChangeListener {

    @Override
    public void onChange(String serviceName, List<RpcRegisterMessage> messages) {
      if (serviceName.equals(RegisterServiceName.MASTER)) {
        refreshMasters(messages);
      } else if (serviceName.equals(RegisterServiceName.WORKER)) {
        refreshWorkers(messages);
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
