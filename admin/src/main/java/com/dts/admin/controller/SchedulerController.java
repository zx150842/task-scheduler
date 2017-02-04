package com.dts.admin.controller;

import com.google.common.collect.Lists;

import com.dts.admin.common.constant.Constant;
import com.dts.admin.common.dto.JobDto;
import com.dts.admin.common.dto.MasterNodeDto;
import com.dts.admin.common.vo.CronJob;
import com.dts.admin.service.CronJobService;
import com.dts.admin.service.SchedulerService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;
import static com.dts.admin.common.constant.Constant.*;

/**
 * @author zhangxin
 */
@Controller
@RequestMapping("/scheduler")
public class SchedulerController {
  @Autowired
  private SchedulerService schedulerService;

  @RequestMapping("")
  public String index(ModelMap model) {
    List<MasterNodeDto> list = schedulerService.getAll();
    list = Lists.newArrayList(new MasterNodeDto("127.0.0.1", 1001));
    model.put(Constant.LIST, list);
    return "scheduler";
  }
}
