package com.dts.scheduler.queue.mysql.dao;

import com.dts.core.TriggeredTaskInfo;

/**
 * @author zhangxin
 */
public interface FinishedTaskDao {
  int add(TriggeredTaskInfo vo);
}
