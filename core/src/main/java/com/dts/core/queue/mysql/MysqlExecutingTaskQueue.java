package com.dts.core.queue.mysql;

import com.dts.core.TriggeredTaskInfo;
import com.dts.core.queue.ExecutingTaskQueue;
import com.dts.core.queue.mysql.impl.AbstractSqlQueue;
import com.dts.core.queue.mysql.impl.CronTaskDao;
import com.dts.core.util.MybatisUtil;
import org.apache.ibatis.session.SqlSession;

import java.util.List;

/**
 * @author zhangxin
 */
public class MysqlExecutingTaskQueue extends AbstractSqlQueue implements ExecutingTaskQueue {
  private static final String PREFIX = CronTaskDao.class.getName();

  @Override public boolean remove(String sysId) {
    return super.remove(sysId, PREFIX);
  }

  @Override public boolean add(TriggeredTaskInfo task) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      int count = sqlSession.update(PREFIX + ".changeToExecuting", task.getSysId());
      sqlSession.commit();
      return count > 0;
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }

  @Override public List<TriggeredTaskInfo> getByTaskId(String taskId) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      List<TriggeredTaskInfo> tasks = sqlSession.selectList(PREFIX + ".getExecutingByTaskId", taskId);
      return tasks;
    } catch (Exception e) {
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }
}
