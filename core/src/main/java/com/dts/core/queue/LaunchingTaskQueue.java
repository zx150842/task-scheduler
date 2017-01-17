package com.dts.core.queue;

import com.dts.core.TaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public interface LaunchingTaskQueue {

  boolean add(TaskInfo task);

  boolean remove(String id);

  List<TaskInfo> getByTaskId(String taskId);
}
