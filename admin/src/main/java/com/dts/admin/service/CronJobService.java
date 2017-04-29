package com.dts.admin.service;

import com.dts.admin.common.vo.TaskLog;
import com.dts.admin.dao.ExecuteTaskDao;
import com.dts.admin.dao.FinishTaskDao;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.dts.admin.common.dto.JobDto;
import com.dts.admin.common.dto.TaskDto;
import com.dts.admin.common.vo.CronJob;
import com.dts.admin.dao.CronJobDao;
import com.dts.core.JobStatus;
import com.dts.core.util.IdUtil;
import com.dts.core.util.Tuple2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * @author zhangxin
 */
@Service
public class CronJobService {

  @Autowired
  private CronJobDao cronJobDao;
  @Autowired
  private ExecuteTaskDao executeTaskDao;
  @Autowired
  private FinishTaskDao finishTaskDao;

  private ClientEndpoint clientEndpoint = ClientEndpoint.endpoint();

  private static final Gson GSON = new Gson();

  public int add(JobDto jobDto) {
    String jobId = "job-" + IdUtil.getUniqId();
    String taskId = "task-" + IdUtil.getUniqId();
    jobDto.setJobId(jobId);
    jobDto.setTaskId(taskId);
    jobDto.setStatus(JobStatus.VALID);
    CronJob cronJob = convertToCronJob(jobDto);
    return cronJobDao.add(cronJob);
  }

  public int update(JobDto jobDto) {
    CronJob cronJob = convertToCronJob(jobDto);
    return cronJobDao.update(cronJob);
  }

  public int delete(String jobId) {
    return cronJobDao.delete(jobId);
  }


  public int pauseOrRun(String jobId, int status) {
    int oldStatus = cronJobDao.getStatus(jobId);
    if (oldStatus == JobStatus.PAUSED && status == JobStatus.VALID) {
      // do nothing
    } else if (oldStatus == JobStatus.VALID && status == JobStatus.PAUSED) {
      // do nothing
    } else {
      return 0;
    }
    return cronJobDao.pauseOrRun(jobId, status);
  }

  public List<JobDto> getAll(String workerGroup, String taskName) {
    List<CronJob> jobs = cronJobDao.getAll(workerGroup, taskName);
    return convertToJobDtos(jobs);
  }

  public String manualTrigger(String jobId, boolean runOnSeed) {
    CronJob cronJob = cronJobDao.getByJobId(jobId);
    if (cronJob == null) {
      return null;
    }
    return clientEndpoint.triggerJob(cronJob, runOnSeed);
  }

  public TaskLog getTaskLog(String sysId) {
    TaskLog log = executeTaskDao.getBySysId(sysId);
    if (log == null) {
      log = finishTaskDao.getBySysId(sysId);
    }
    return log;
  }

  public List<String> getWorkerGroups() {
    return clientEndpoint.getWorkerGroups();
  }

  public Set<Tuple2<String, String>> getTasks(String workerGroup) {
    return clientEndpoint.getTasks(workerGroup);
  }

  public boolean refreshJobs() throws Exception {
    return clientEndpoint.refreshJobs();
  }

  private List<JobDto> convertToJobDtos(List<CronJob> list) {
    List<JobDto> result = Lists.newArrayList();
    for (CronJob job : list) {
      List<TaskDto> tasks = GSON.fromJson(job.getTasks(), new TypeToken<List<TaskDto>>(){}.getType());
      if (tasks == null) {
        continue;
      }
      TaskDto task = tasks.get(0);
      JobDto jobDto = new JobDto();
      jobDto.setJobId(job.getJobId());
      jobDto.setTaskId(task.getTaskId());
      jobDto.setTaskName(task.getTaskName());
      jobDto.setParams(task.getParams());
      jobDto.setCronExpression(job.getCronExpression());
      jobDto.setDesc(job.getDesc());
      jobDto.setMaxRunTimeSec(job.getMaxRunTimeSec());
      jobDto.setWorkerGroup(job.getWorkerGroup());
      jobDto.setStatus(job.getStatus());
      result.add(jobDto);
    }
    return result;
  }

  private CronJob convertToCronJob(JobDto jobDto) {
    String task = String.format("[{\"taskId\":\"%s\",\"taskName\":\"%s\",\"params\":\"%s\"}]",
        jobDto.getTaskId(), jobDto.getTaskName(), jobDto.getParams());
    CronJob cronJob = new CronJob(jobDto.getJobId(), task, jobDto.getCronExpression(),
        jobDto.getWorkerGroup(), jobDto.getStatus(), -1, new Date());
    cronJob.setDesc(jobDto.getDesc());
    return cronJob;

  }
}
