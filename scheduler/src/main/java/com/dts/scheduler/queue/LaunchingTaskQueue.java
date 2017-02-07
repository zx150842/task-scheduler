package com.dts.scheduler.queue;

import com.dts.core.TriggeredTaskInfo;

/**
 * @author zhangxin
 */
public interface LaunchingTaskQueue {

  boolean add(TriggeredTaskInfo task);

  boolean remove(String id);

  boolean updateWorkerId(String sysId, String workerId);
}
