package com.dts.core.queue;

import com.dts.core.TaskInfo;

/**
 * @author zhangxin
 */
public interface LaunchedTaskQueue {

  boolean add(TaskInfo task);

  boolean remove(String id);
}
