package com.dts.core.queue.mysql;

import com.dts.core.TaskInfo;
import com.dts.core.queue.LaunchedTaskQueue;

/**
 * @author zhangxin
 */
public class MysqlLaunchedTaskQueue implements LaunchedTaskQueue {
  @Override public boolean add(TaskInfo task) {
    return false;
  }

  @Override public boolean remove(String id) {
    return false;
  }
}
