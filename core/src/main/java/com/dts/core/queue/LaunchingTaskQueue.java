package com.dts.core.queue;

import com.dts.core.TriggeredTaskInfo;

import java.util.List;

/**
 * @author zhangxin
 */
public interface LaunchingTaskQueue {

  boolean add(TriggeredTaskInfo task);

  boolean remove(String id);
}
