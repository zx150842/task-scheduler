package com.dts.admin.controller;

import com.google.common.base.Throwables;
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
import org.springframework.web.bind.annotation.ResponseBody;

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
    model.put(Constant.LIST, list);
    return "scheduler";
  }

  @RequestMapping("/refresh")
  @ResponseBody
  public String refresh() {
    String result;
    try {
      result = schedulerService.refreshMaster() ? Constant.SUCCESS : Constant.FAIL;
    } catch (Exception e) {
      result = Throwables.getStackTraceAsString(e);
    }
    return result;
  }
}
