package com.dts.executor.task;

import org.springframework.stereotype.Service;

/**
 * @author zhangxin
 */
@Service
public class TestClass {

  @Task("logconsoleWithNoParam")
  public void method1() {
    System.out.println("do logconsoleWithNoParam");
  }

  @Task("logconsoleWithParam")
  public void method2(String t1, int t2, boolean t3) {
    System.out.println("do logconsoleWithParam");
  }

  @Task("task3")
  public void method3(String t1) {

  }
}
