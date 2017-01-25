package com.dts.core.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.queue.FinishedTaskQueue;
import com.dts.core.queue.mysql.impl.AbstractSqlQueue;
import com.dts.core.queue.mysql.impl.FinishedTaskDao;

/**
 * @author zhangxin
 */
public class MysqlFinishedTaskQueue extends AbstractSqlQueue implements FinishedTaskQueue {
  private static final String PREFIX = FinishedTaskDao.class.getName();

  @Override public boolean add(TriggeredTaskInfo task) {
    return super.add(task, PREFIX);
  }
}
