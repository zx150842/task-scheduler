package com.dts.core.queue.kafka;

import com.dts.core.TaskInfo;
import com.dts.core.queue.LaunchedTaskQueue;

/**
 * @author zhangxin
 */
public class KafkaLaunchedTaskQueue implements LaunchedTaskQueue {
  @Override public boolean add(TaskInfo task) {
    return false;
  }

  @Override public boolean remove(String id) {
    return false;
  }
}
