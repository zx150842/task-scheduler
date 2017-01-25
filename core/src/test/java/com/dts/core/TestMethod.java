package com.dts.core;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;

/**
 * @author zhangxin
 */
public class TestMethod {

  @Test
  public void test() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    Obj obj = new Obj();
    Method m = Obj.class.getDeclaredMethod("method1", int.class, String.class, boolean.class);
    m.invoke(obj, 1, "hello world", true);
  }

  @Test
  public void test2() {
    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
//    map.put("int", "1");
//    map.put("String", "hello world");
//    map.put("boolean", "true");
    System.out.println(StringUtils.substring(map.toString(), 1, map.toString().length() - 1));
  }

  class Obj {
    public void method1(int a, String b, boolean c) {
      System.out.println("int a: " + a + ", String b: " + b + ", boolean c:");
    }
  }
}
