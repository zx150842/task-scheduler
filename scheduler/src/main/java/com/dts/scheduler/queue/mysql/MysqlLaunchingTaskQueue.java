package com.dts.scheduler.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import com.dts.scheduler.MybatisUtil;
import com.dts.scheduler.queue.LaunchingTaskQueue;
import com.dts.scheduler.queue.mysql.impl.CronTaskDao;

import com.google.common.collect.Maps;
import org.apache.ibatis.session.SqlSession;

import com.dts.scheduler.queue.mysql.impl.AbstractSqlQueue;

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

  @Override
  public boolean updateWorkerId(String sysId, String workerId) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      Map<String, String> params = Maps.newHashMap();
      params.put("sysId", sysId);
      params.put("workerId", workerId);
      int count = sqlSession.update(PREFIX + ".updateWorkerId", params);
      sqlSession.commit();
      return count > 0;
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }
}
