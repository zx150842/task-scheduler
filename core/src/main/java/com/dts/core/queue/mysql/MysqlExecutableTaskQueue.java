package com.dts.core.queue.mysql;

import com.dts.core.TaskInfo;
import com.dts.core.queue.ExecutableTaskQueue;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * @author zhangxin
 */
public class MysqlExecutableTaskQueue extends JdbcDaoSupport implements ExecutableTaskQueue {
  @Override public boolean add(TaskInfo task) {
    return false;
  }

  @Override public boolean resume(TaskInfo task) {
    return false;
  }

  @Override public boolean remove(String id) {
    return false;
  }

  @Override public List<TaskInfo> getManualTriggerTasks(String workerGroup) {
    return null;
  }

  @Override public List<TaskInfo> getAutoTriggerTasks(String workerGroup) {
    return null;
  }

  @Override public TaskInfo getById(String id) {
    return null;
  }
}
