package com.dts.executor.task;

import com.dts.core.util.DataTypeUtil;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

/**
 * @author zhangxin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:applicationContext.xml")
public class TaskScannerTest {

  @Autowired
  private TaskScanner taskScanner;

  @Test
  public void testGetClasses() {
    List<Class> list = taskScanner.getClasses("com.dts.executor");
    for (Class clazz : list) {
      System.out.println(clazz);
    }
  }

  @Test
  public void testGetTaskMethodWrapper() {
    TaskMethodWrapper tw = taskScanner.getTaskMethodWrapper("com.dts.executor");
    for (String taskName : tw.taskMethodDetails.keySet()) {
      System.out.println(taskName + " " + tw.taskMethodDetails.get(taskName));
    }
  }

  @Test
  public void testDeserializeTaskParams() {
    TaskMethodWrapper tw = taskScanner.getTaskMethodWrapper("com.dts.executor");
    String taskName = "task1";
    String[] params = {"hello world", "1"};
    Method method = tw.taskMethods.get(taskName);
    for (int i = 0; i < method.getParameters().length; ++i) {
      Parameter param = method.getParameters()[i];
      Object o = DataTypeUtil.convertToPrimitiveType(param.getType().getSimpleName(), StringUtils.trimToEmpty(params[i]));
      System.out.println(o.getClass().getSimpleName() + " " + o);
    }
  }
}
