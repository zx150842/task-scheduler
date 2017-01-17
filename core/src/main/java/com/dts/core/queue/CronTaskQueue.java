package com.dts.core.queue;

import com.dts.core.TaskConf;

import java.util.List;

/**
 * @author zhangxin
 */
public interface CronTaskQueue {

  boolean add(TaskConf task);

  boolean update(TaskConf task);

  boolean remove(String taskId);

  List<TaskConf> getAllValid();

  boolean addBatch(List<TaskConf> tasks);

  boolean updateBatch(List<TaskConf> tasks);

  boolean removeBatch(List<String> taskIds);

  List<String> getTaskId(String taskGroupId);

  boolean containTask(String taskId);

  boolean containTaskGroup(String taskGroupId);

  TaskConf getTask(String taskId);

  List<TaskConf> getTasks(String taskGroupId);
}
