package com.dts.executor.example.service;

import com.dts.executor.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author zhangxin
 */
@Service
public class ExampleService {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Task("logconsoleWithNoParam")
  public void logConsoleWithNoParamService() {
    logger.info("log console with no param service");
  }

  @Task("logconsoleWithParam")
  public void logConsoleWithParamService(String t1, int t2, boolean t3) {
    logger.info("log console with param service: String {}, int {}, boolean {}",
      t1, t2, t3);
  }
}
