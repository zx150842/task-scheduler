package com.dts.scheduler.queue.mysql.impl;

import com.dts.core.TriggeredTaskInfo;

import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author zhangxin
 */
public interface CronTaskDao {

  int add(TriggeredTaskInfo vo);

  int changeToExecutable(@Param("sysId") String sysId);

  int changeToLaunching(@Param("sysId") String sysId);

  int changeTolaunched(@Param("sysId") String sysId);

  int changeToExecuting(@Param("sysId") String sysId);

  int delete(@Param("sysId") String sysId);

  List<TriggeredTaskInfo> getExecutableByWorkerGroup(@Param("workerGroup") String workerGroup, @Param("manual") boolean manual);

  List<TriggeredTaskInfo> getExecutingByTaskId(@Param("taskId") String taskId);

  List<TriggeredTaskInfo> getBySysId(@Param("sysId") String sysId);

  int updateWorkerId(@Param("sysId") String sysId, @Param("workerId") String workerId);
}
