package com.dts.scheduler;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;

/**
 * @author zhangxin
 */
public class MybatisUtil {

  private static SqlSessionFactory sqlSessionFactory;

  private static SqlSessionFactory create() {
    if (sqlSessionFactory == null) {
      synchronized (MybatisUtil.class) {
        if (sqlSessionFactory == null) {
          InputStream in = MybatisUtil.class.getResourceAsStream("/mybatis-config.xml");
          sqlSessionFactory = new SqlSessionFactoryBuilder().build(in);
        }
      }
    }
    return sqlSessionFactory;
  }

  public static SqlSession getSqlSession() {
    SqlSessionFactory factory = create();
    return factory.openSession();
  }

  public static void closeSqlSession(SqlSession sqlSession) {
    if (sqlSession != null) {
      sqlSession.close();
    }
  }
}
