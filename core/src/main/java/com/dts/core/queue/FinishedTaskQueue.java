package com.dts.core.queue;

import com.dts.core.TriggeredTaskInfo;

/**
 * @author zhangxin
 */
public interface FinishedTaskQueue {

  boolean add(TriggeredTaskInfo task);
}
