package com.dts.core.queue.mysql;

import com.dts.core.TaskInfo;
import com.dts.core.queue.ExecutingTaskQueue;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import java.util.List;

/**
 * @author zhangxin
 */
public class MysqlExecutingTaskQueue extends JdbcDaoSupport implements ExecutingTaskQueue {
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
