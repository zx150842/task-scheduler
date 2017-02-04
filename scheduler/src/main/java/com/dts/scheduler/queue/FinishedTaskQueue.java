package com.dts.scheduler.queue;

import com.dts.core.TriggeredTaskInfo;

/**
 * @author zhangxin
 */
public interface FinishedTaskQueue {

  boolean add(TriggeredTaskInfo task);
}
