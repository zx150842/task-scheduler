package com.dts.admin.service;

import com.dts.admin.common.dto.WorkerNodeDto;

import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zhangxin
 */
@Service
public class ExecutorService {

  private ClientEndpoint clientEndpoint = ClientEndpoint.endpoint();

  public List<WorkerNodeDto> getOnlineWorkers() {
    return clientEndpoint.getOnlineWorkers();
  }

  public List<WorkerNodeDto> getSeedWorkers() {
    return clientEndpoint.getSeedWorkers();
  }

  public boolean refreshWorkers() throws Exception {
    return clientEndpoint.refreshMasterWorkers();
  }
}
