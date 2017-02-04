package com.dts.scheduler.queue.mysql;

import com.dts.core.TriggeredTaskInfo;

import com.dts.scheduler.queue.FinishedTaskQueue;
import com.dts.scheduler.queue.mysql.impl.AbstractSqlQueue;
import com.dts.scheduler.queue.mysql.impl.FinishedTaskDao;

/**
 * @author zhangxin
 */
public class MysqlFinishedTaskQueue extends AbstractSqlQueue implements FinishedTaskQueue {
  private static final String PREFIX = FinishedTaskDao.class.getName();

  @Override public boolean add(TriggeredTaskInfo task) {
    return super.add(task, PREFIX);
  }
}
