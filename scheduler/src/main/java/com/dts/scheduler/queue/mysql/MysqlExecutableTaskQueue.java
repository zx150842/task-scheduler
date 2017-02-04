package com.dts.scheduler.queue.mysql;

import com.google.common.collect.Maps;

import com.dts.core.TriggeredTaskInfo;
import com.dts.scheduler.MybatisUtil;
import com.dts.scheduler.queue.mysql.impl.AbstractSqlQueue;
import com.dts.scheduler.queue.mysql.impl.CronTaskDao;

import org.apache.ibatis.session.SqlSession;

import java.util.List;
import java.util.Map;

import com.dts.scheduler.queue.ExecutableTaskQueue;

/**
 * @author zhangxin
 */
public class MysqlExecutableTaskQueue extends AbstractSqlQueue implements ExecutableTaskQueue {
  private static final String PREFIX = CronTaskDao.class.getName();

  @Override public boolean add(TriggeredTaskInfo task) {
    return super.add(task, PREFIX);
  }

  @Override public boolean resume(TriggeredTaskInfo task) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      int count = sqlSession.update(PREFIX + ".changeToExecutable", task.getSysId());
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

  @Override public List<TriggeredTaskInfo> getManualTriggerTasks(String workerGroup) {
    return getTriggerTasks(workerGroup, true);
  }

  @Override public List<TriggeredTaskInfo> getAutoTriggerTasks(String workerGroup) {
    return getTriggerTasks(workerGroup, false);
  }

  private List<TriggeredTaskInfo> getTriggerTasks(String workerGroup, boolean isManual) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      Map<String, Object> params = Maps.newHashMap();
      params.put("workerGroup", workerGroup);
      params.put("manual", isManual);
      List<TriggeredTaskInfo> tasks = sqlSession.selectList(PREFIX + ".getExecutableByWorkerGroup", params);
      return tasks;
    } catch (Exception e) {
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }
}
