package com.dts.core.worker;

import java.lang.annotation.*;

/**
 * @author zhangxin
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Task {
  String name();
}
