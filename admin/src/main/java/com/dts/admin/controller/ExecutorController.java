package com.dts.admin.controller;

import com.dts.admin.common.constant.Constant;
import com.dts.admin.common.dto.WorkerNodeDto;
import com.dts.admin.service.ExecutorService;

import com.google.common.base.Throwables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * @author zhangxin
 */
@Controller
@RequestMapping("/executor")
public class ExecutorController {
  @Autowired
  private ExecutorService executorService;

  @RequestMapping("/online")
  public String online(ModelMap model) {
    List<WorkerNodeDto> list = executorService.getOnlineWorkers();
    model.put(Constant.LIST, list);
    return "executor/online-executor";
  }

  @RequestMapping("/seed")
  public String seed(ModelMap model) {
    List<WorkerNodeDto> list = executorService.getSeedWorkers();
    model.put(Constant.LIST, list);
    return "executor/seed-executor";
  }

  @RequestMapping("/refresh")
  @ResponseBody
  public String refresh() {
    String result;
    try {
      result = executorService.refreshWorkers() ? Constant.SUCCESS : Constant.FAIL;
    } catch (Exception e) {
      result = Throwables.getStackTraceAsString(e);
    }
    return result;
  }
}
