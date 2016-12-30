package com.dts.core.queue;

import com.dts.core.TaskInfo;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * @author zhangxin
 */
public interface CronTaskQueue {

  boolean add(TaskInfo task);

  boolean update(TaskInfo task);

  boolean remove(String taskId);

  List<TaskInfo> getAllValid();

  boolean addBatch(List<TaskInfo> tasks);

  boolean updateBatch(List<TaskInfo> tasks);

  boolean removeBatch(List<String> taskIds);

  List<String> getTaskId(String taskGroupId);
}
