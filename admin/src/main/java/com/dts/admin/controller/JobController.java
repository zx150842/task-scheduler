package com.dts.admin.controller;

import com.dts.admin.common.constant.Constant;
import com.dts.admin.common.dto.JobDto;
import com.dts.admin.service.CronJobService;
import com.dts.core.util.Tuple2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Collections;
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
    if (workerGroup != null) {
      Set<Tuple2<String, String>> tasks = cronJobService.getTasks(workerGroup);
      model.put(Constant.TASKS, tasks);
    }
    if (workerGroups != null && !workerGroups.isEmpty()) {
      Set<Tuple2<String, String>> firstGroupTasks = cronJobService.getTasks(workerGroups.get(0));
      model.put(Constant.FIRST_GROUP_TASKS, firstGroupTasks);
    }
    model.put(Constant.LIST, jobs);
    model.put(Constant.WORKER_GROUPS, workerGroups);
    return "job";
  }

  @RequestMapping("/tasks")
  @ResponseBody
  public Set<Tuple2<String, String>> tasks(String workerGroup) {
    Set<Tuple2<String, String>> tasks = cronJobService.getTasks(workerGroup);
    if (tasks == null || tasks.isEmpty()) {
      return Collections.emptySet();
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
        && (isAdd || (jobDto.getJobId() != null && jobDto.getTaskId() != null))
        && jobDto.getCronExpression() != null
        && jobDto.getTaskName() != null
        && jobDto.getWorkerGroup() != null;
    return valid;
  }
}
