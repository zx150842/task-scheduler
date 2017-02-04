package com.dts.admin.service;

import com.dts.admin.common.dto.MasterNodeDto;

import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zhangxin
 */
@Service
public class SchedulerService {
  private ClientEndpoint clientEndpoint = ClientEndpoint.endpoint();

  public List<MasterNodeDto> getAll() {
    return clientEndpoint.getMasters();
  }
}
