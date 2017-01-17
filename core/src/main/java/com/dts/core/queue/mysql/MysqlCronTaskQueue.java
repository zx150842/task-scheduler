package com.dts.core.queue.mysql;

import com.dts.core.TaskConf;
import com.dts.core.queue.CronTaskQueue;

import java.util.List;

/**
 * @author zhangxin
 */
public class MysqlCronTaskQueue implements CronTaskQueue {
  @Override public boolean add(TaskConf task) {
    return false;
  }

  @Override public boolean update(TaskConf task) {
    return false;
  }

  @Override public boolean remove(String taskId) {
    return false;
  }

  @Override public List<TaskConf> getAllValid() {
    return null;
  }

  @Override public boolean addBatch(List<TaskConf> tasks) {
    return false;
  }

  @Override public boolean updateBatch(List<TaskConf> tasks) {
    return false;
  }

  @Override public boolean removeBatch(List<String> taskIds) {
    return false;
  }

  @Override public List<String> getTaskId(String taskGroupId) {
    return null;
  }

  @Override public boolean containTask(String taskId) {
    return false;
  }

  @Override public boolean containTaskGroup(String taskGroupId) {
    return false;
  }

  @Override public TaskConf getTask(String taskId) {
    return null;
  }

  @Override public List<TaskConf> getTasks(String taskGroupId) {
    return null;
  }
}
