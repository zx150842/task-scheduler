package com.dts.core.queue.mysql;

import com.dts.core.TaskInfo;
import com.dts.core.queue.FinishedTaskQueue;

/**
 * @author zhangxin
 */
public class MysqlFinishedTaskQueue implements FinishedTaskQueue {
  @Override public boolean add(TaskInfo task) {
    return false;
  }
}
