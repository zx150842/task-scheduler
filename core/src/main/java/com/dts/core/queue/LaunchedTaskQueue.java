package com.dts.core.queue;

import com.dts.core.TriggeredTaskInfo;

/**
 * @author zhangxin
 */
public interface LaunchedTaskQueue {

  boolean add(TriggeredTaskInfo task);

  boolean remove(String id);
}
