package com.dts.core.queue.mysql;

import com.dts.core.JobConf;
import com.dts.core.TaskConf;
import com.dts.core.TriggeredTaskInfo;
import com.dts.core.queue.ExecutableTaskQueue;
import com.dts.core.queue.mysql.impl.AbstractSqlQueue;
import com.dts.core.queue.mysql.impl.CronJob;
import com.dts.core.queue.mysql.impl.CronTask;
import com.dts.core.queue.mysql.impl.CronTaskDao;
import com.dts.core.util.MybatisUtil;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.ibatis.session.SqlSession;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
