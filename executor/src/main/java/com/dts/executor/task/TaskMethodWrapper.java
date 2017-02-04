package com.dts.executor.task;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * @author zhangxin
 */
public class TaskMethodWrapper {
  // taskName -> method
  public final Map<String, Method> taskMethods;
  // taskName -> method desc
  public final Map<String, String> taskMethodDetails;
  // taskName -> bean
  public final Map<String, Object> taskBeans;

  public TaskMethodWrapper(
      Map<String, Method> taskMethods,
      Map<String, String> taskMethodDetails,
      Map<String, Object> taskBeans) {
    this.taskMethods = taskMethods;
    this.taskMethodDetails = taskMethodDetails;
    this.taskBeans = taskBeans;
  }
}
