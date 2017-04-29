package com.dts.admin.controller;

import com.dts.admin.common.constant.Constant;
import com.dts.admin.common.vo.TaskLog;
import com.dts.admin.service.TaskLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * @author zhangxin
 */
@Controller
@RequestMapping("/log")
public class LogController {
  @Autowired
  private TaskLogService taskLogService;

  @RequestMapping("/execute")
  public String getExecuteLog(ModelMap model) {
    List<TaskLog> list = taskLogService.getExecuteTaskLog();
    model.put(Constant.LIST, list);
    return "log/execute-log";
  }

  @RequestMapping("/finish")
  public String getFinishLog(ModelMap model) {
    List<TaskLog> list = taskLogService.getLastFinishTaskLog();
    model.put(Constant.LIST, list);
    return "log/finish-log";
  }
}
