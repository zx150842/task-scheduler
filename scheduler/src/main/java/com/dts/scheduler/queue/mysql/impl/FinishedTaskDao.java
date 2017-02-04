package com.dts.scheduler.queue.mysql.impl;

import com.dts.core.TriggeredTaskInfo;

/**
 * @author zhangxin
 */
public interface FinishedTaskDao {
  int add(TriggeredTaskInfo vo);
}
