package com.dts.core.queue.mysql.impl;

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

  int updateTriggerTime(@Param("jobId") String jobId, @Param("lastTriggerTime")Date lastTriggerTime);
}
