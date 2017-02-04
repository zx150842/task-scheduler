package com.dts.executor.task;

import org.springframework.stereotype.Service;

/**
 * @author zhangxin
 */
@Service
public class TestClass {

  @Task("task1")
  public void method1(String t1, int t2) {

  }

  @Task("task2")
  public void method2() {

  }

  @Task("task3")
  public void method3(String t1) {

  }
}
