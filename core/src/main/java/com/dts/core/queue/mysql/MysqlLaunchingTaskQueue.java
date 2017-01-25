package com.dts.core.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.queue.LaunchingTaskQueue;
import com.dts.core.queue.mysql.impl.AbstractSqlQueue;
import com.dts.core.queue.mysql.impl.CronTaskDao;
import com.dts.core.util.MybatisUtil;
import com.google.common.collect.Maps;
import org.apache.ibatis.session.SqlSession;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

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
