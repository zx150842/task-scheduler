package com.dts.core.queue.kafka;

import com.dts.core.TaskInfo;
import com.dts.core.queue.ExecutingTaskQueue;

import java.util.List;

/**
 * @author zhangxin
 */
public class KafkaExecutingTaskQueue implements ExecutingTaskQueue {
  @Override public boolean add(TaskInfo task) {
    return false;
  }

  @Override public boolean remove(String id) {
    return false;
  }

  @Override public List<TaskInfo> getByTaskId(String taskId) {
    return null;
  }
}
