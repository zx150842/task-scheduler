package com.dts.core.queue.mysql;

import com.dts.core.queue.ExecutingTaskQueue;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * @author zhangxin
 */
public class MysqlExecutingTaskQueue extends JdbcDaoSupport implements ExecutingTaskQueue {
}
