package com.dts.admin.service;

import com.dts.admin.common.vo.TaskLog;
import com.dts.admin.dao.ExecuteTaskDao;
import com.dts.admin.dao.FinishTaskDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zhangxin
 */
@Service
public class TaskLogService {
  @Autowired
  private FinishTaskDao finishTaskDao;
  @Autowired
  private ExecuteTaskDao executeTaskDao;

  public List<TaskLog> getLastFinishTaskLog() {
    return finishTaskDao.getLast();
  }

  public List<TaskLog> getExecuteTaskLog() {
    return executeTaskDao.getAll();
  }
}
