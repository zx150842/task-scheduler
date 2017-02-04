package com.dts.scheduler.queue;

import com.dts.core.TriggeredTaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public interface ExecutableTaskQueue {

  boolean add(TriggeredTaskInfo task);

  boolean resume(TriggeredTaskInfo task);

  boolean remove(String id);

  List<TriggeredTaskInfo> getManualTriggerTasks(String workerGroup);

  List<TriggeredTaskInfo> getAutoTriggerTasks(String workerGroup);
}
