package com.dts.admin.controller;

import com.google.common.collect.Lists;

import com.dts.admin.common.constant.Constant;
import com.dts.admin.common.dto.WorkerNodeDto;
import com.dts.admin.service.ExecutorService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * @author zhangxin
 */
@Controller
public class ExecutorController {
  @Autowired
  private ExecutorService executorService;

  @RequestMapping("/executor")
  public String index(ModelMap model) {
    List<WorkerNodeDto> list = executorService.getAll();
    list = Lists.newArrayList(new WorkerNodeDto("127.0.0.1", 1000, "testWorkerId1",
        "engine", 10, null));
    model.put(Constant.LIST, list);
    return "/executor";
  }
}
