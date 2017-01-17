package com.dts.core.queue.kafka;

import com.dts.core.TaskInfo;
import com.dts.core.queue.FinishedTaskQueue;

/**
 * @author zhangxin
 */
public class KafkaFinishedTaskQueue implements FinishedTaskQueue {
  @Override public boolean add(TaskInfo task) {
    return false;
  }
}
