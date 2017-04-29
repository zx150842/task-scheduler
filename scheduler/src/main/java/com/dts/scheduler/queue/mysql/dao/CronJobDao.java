package com.dts.scheduler.queue.mysql.dao;

import com.dts.scheduler.queue.mysql.vo.CronJob;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

/**
 * @author zhangxin
 */
public interface CronJobDao {
  int add(CronJob vo);

  int update(CronJob vo);

  int delete(@Param("jobId") String jobId);

  List<CronJob> getAllValid();

  int updateTriggerTime(@Param("jobId") String jobId, @Param("lastTriggerTime") Date lastTriggerTime);

  List<CronJob> getByJobId(@Param("jobId") String jobId);
}
