package com.dts.scheduler.queue.mysql.dao;

import com.dts.core.TriggeredTaskInfo;

import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author zhangxin
 */
public interface CronTaskDao {

  int add(TriggeredTaskInfo vo);

  int changeToExecuting(TriggeredTaskInfo task);

  int delete(@Param("sysId") String sysId);

  List<TriggeredTaskInfo> getExecutableByWorkerGroup(@Param("workerGroup") String workerGroup, @Param("manual") boolean manual);

  List<TriggeredTaskInfo> getExecutingByTaskId(@Param("taskId") String taskId);

  List<TriggeredTaskInfo> getBySysId(@Param("sysId") String sysId);

  int updateWorkerId(@Param("sysId") String sysId, @Param("workerId") String workerId);

  List<TriggeredTaskInfo> getAllExecuting();

  int resume(@Param("sysId") String sysId);

  List<TriggeredTaskInfo> getAllExecutable();
}
