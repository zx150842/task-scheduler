package com.dts.core.queue;

import com.dts.core.TaskInfo;

/**
 * @author zhangxin
 */
public interface FinishedTaskQueue {

  boolean add(TaskInfo task);
}
