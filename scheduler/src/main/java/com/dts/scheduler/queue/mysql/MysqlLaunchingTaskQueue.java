package com.dts.scheduler.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import com.dts.scheduler.MybatisUtil;
import com.dts.scheduler.queue.LaunchingTaskQueue;
import com.dts.scheduler.queue.mysql.impl.CronTaskDao;

import org.apache.ibatis.session.SqlSession;

import com.dts.scheduler.queue.mysql.impl.AbstractSqlQueue;

/**
 * @author zhangxin
 */
public class MysqlLaunchingTaskQueue extends AbstractSqlQueue implements LaunchingTaskQueue {
  private static final String PREFIX = CronTaskDao.class.getName();

  @Override public boolean add(TriggeredTaskInfo task) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      int count = sqlSession.update(PREFIX + ".changeToLaunching", task.getSysId());
      sqlSession.commit();
      return count > 0;
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }

  @Override public boolean remove(String sysId) {
    return true;
  }
}
