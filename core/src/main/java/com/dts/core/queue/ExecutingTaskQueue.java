package com.dts.core.queue;

import com.dts.core.TriggeredTaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public interface ExecutingTaskQueue {

  boolean add(TriggeredTaskInfo task);

  boolean remove(String id);

  List<TriggeredTaskInfo> getByTaskId(String taskId);
}
