package com.dts.admin.dao;

import com.dts.admin.common.vo.CronJob;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author zhangxin
 */
@Repository
public interface CronJobDao {

  int add(CronJob vo);

  int update(CronJob vo);

  int delete(@Param("jobId") String jobId);

  int pauseOrRun(@Param("jobId") String jobId, @Param("status") int status);

  List<CronJob> getAll(@Param("workerGroup") String workerGroup, @Param("taskName") String taskName);

  CronJob getByJobId(@Param("jobId") String jobId);

  int getStatus(@Param("jobId") String jobId);
}
