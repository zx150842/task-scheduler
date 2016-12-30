package com.dts.core.queue.mysql;

import com.dts.core.queue.ExecutableTaskQueue;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

/**
 * @author zhangxin
 */
public class MysqlExecutableTaskQueue extends JdbcDaoSupport implements ExecutableTaskQueue {
}
