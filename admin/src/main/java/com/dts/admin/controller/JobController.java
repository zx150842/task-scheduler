package com.dts.admin.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.dts.admin.common.constant.Constant;
import com.dts.admin.common.dto.JobDto;
import com.dts.admin.common.vo.CronJob;
import com.dts.admin.service.CronJobService;
import com.dts.core.JobConf;
import com.dts.core.util.Tuple2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Set;

import static com.dts.admin.common.constant.Constant.FAIL;
import static com.dts.admin.common.constant.Constant.SUCCESS;

/**
 * @author zhangxin
 */
@Controller
@RequestMapping("/job")
public class JobController {
  @Autowired
  private CronJobService cronJobService;

  @RequestMapping("")
  public String index(ModelMap model, String workerGroup, String taskName) {
    model.put("searchWorkerGroup", workerGroup);
    model.put("searchTaskName", taskName);
    if (Constant.SELECTOR_ALL.equals(workerGroup)) {
      workerGroup = null;
    }
    if (Constant.SELECTOR_ALL.equals(taskName)) {
      taskName = null;
    }
    List<JobDto> jobs = cronJobService.getAll(workerGroup, taskName);
    List<String> workerGroups = cronJobService.getWorkerGroups();
    workerGroups = Lists.newArrayList("engine", "statis");
    if (workerGroups != null && !workerGroups.isEmpty()) {
      Set<Tuple2<String, String>> tasks = cronJobService.getTasks(workerGroup);
      tasks = Sets.newHashSet(new Tuple2<String, String>("add","String t1,int t2"),
          new Tuple2<String, String>("delete", "int t1"));
      model.put(Constant.TASKS, tasks);
    }
    model.put(Constant.LIST, jobs);
    model.put(Constant.WORKER_GROUPS, workerGroups);
    return "job";
  }

  @RequestMapping("/tasks")
  @ResponseBody
  public Set<Tuple2<String, String>> tasks(String workerGroup) {
    Set<Tuple2<String, String>> tasks = cronJobService.getTasks(workerGroup);
    if (workerGroup.equals("engine")) {
      tasks = Sets.newHashSet(new Tuple2<String, String>("add", "String t1,int t2"),
          new Tuple2<String, String>("delete", "int t1"));
    } else if (workerGroup.equals("statis")) {
      tasks = Sets.newHashSet(new Tuple2<String, String>("update", ""));
    }
    return tasks;
  }

  @RequestMapping("/add")
  @ResponseBody
  public String add(JobDto jobDto) {
    if (!checkJob(jobDto, true)) {
      return "请正确配置job";
    }
    String result = cronJobService.add(jobDto) > 0 ? SUCCESS : FAIL;
    return result;
  }

  @RequestMapping("/update")
  @ResponseBody
  public String update(JobDto jobDto) {
    if (!checkJob(jobDto, false)) {
      return "请正确配置job";
    }
    String result = cronJobService.update(jobDto) > 0 ? SUCCESS : FAIL;
    return result;
  }

  @RequestMapping("/delete")
  @ResponseBody
  public String delete(String jobId) {
    String result = cronJobService.delete(jobId) > 0 ? SUCCESS : FAIL;
    return result;
  }

  @RequestMapping("/pauseOrRun")
  @ResponseBody
  public String pauseOrRun(String jobId, int status) {
    String result = cronJobService.pauseOrRun(jobId, status) > 0 ? SUCCESS : FAIL;
    return result;
  }

  @RequestMapping("/trigger")
  @ResponseBody
  public String manualTrigger(String jobId) {
    String result = cronJobService.manualTrigger(jobId) ? SUCCESS : FAIL;
    return result;
  }

  private boolean checkJob(JobDto jobDto, boolean isAdd) {
    boolean valid =
        jobDto != null
        && (isAdd || jobDto.getJobId() != null)
        && jobDto.getCronExpression() != null
        && jobDto.getTaskName() != null
        && jobDto.getWorkerGroup() != null;
    return valid;
  }
}
