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

  public List<WorkerNodeDto> getAll() {
    return clientEndpoint.getWorkers();
  }
}
