package com.dts.scheduler.queue.mysql.impl;

import com.dts.scheduler.MybatisUtil;

import org.apache.ibatis.session.SqlSession;

/**
 * @author zhangxin
 */
public abstract class AbstractSqlQueue {
  public <T> boolean add(T task, String prefix) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      int count = sqlSession.insert(prefix + ".add", task);
      sqlSession.commit();
      return count > 0;
    } catch (Exception e) {
      sqlSession.rollback();
      throw e;
    } finally {
      MybatisUtil.closeSqlSession(sqlSession);
    }
  }

  public boolean remove(String id, String prefix) {
    SqlSession sqlSession = null;
    try {
      sqlSession = MybatisUtil.getSqlSession();
      int count  = sqlSession.delete(prefix + ".delete", id);
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
