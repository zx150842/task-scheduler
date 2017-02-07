package com.dts.executor.task;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 任务注解，在项目中引用这个注解的方法将会被认为是定时任务的执行方法
 *
 * @author zhangxin
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Task {
  String value() default "";
}
