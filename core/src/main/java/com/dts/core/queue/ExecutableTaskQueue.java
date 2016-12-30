package com.dts.core.queue;

import com.dts.core.TaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public interface ExecutableTaskQueue {

  boolean add(TaskInfo task);

  boolean resume(TaskInfo task);

  boolean remove(String id);

  List<TaskInfo> getManualTriggerTasks(String workerGroup);

  List<TaskInfo> getAutoTriggerTasks(String workerGroup);

  TaskInfo getById(String id);
}
