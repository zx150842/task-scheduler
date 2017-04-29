package com.dts.admin.dao;

import com.dts.admin.common.vo.TaskLog;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author zhangxin
 */
@Repository
public interface ExecuteTaskDao {

  List<TaskLog> getAll();

  TaskLog getBySysId(@Param("sysId") String sysId);
}
