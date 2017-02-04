package com.dts.executor.task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.dts.core.exception.DTSException;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

/**
 * @author zhangxin
 */
@Component
public class TaskScanner implements ApplicationContextAware {

  private ApplicationContext context;

  public TaskMethodWrapper getTaskMethodWrapper(String packageName) {
    List<Class> classes = getClasses(packageName);
    if (classes == null || classes.isEmpty()) {
      return null;
    }
    Map<String, Method> taskMethods = Maps.newHashMap();
    Map<String, String> taskMethodDetails = Maps.newHashMap();
    Map<String, Object> taskBeans = Maps.newHashMap();
    for (Class clazz : classes) {
      for (Method method : clazz.getMethods()) {
        if (method.isAnnotationPresent(Task.class)) {
          try {
            Task taskAnnotation = method.getAnnotation(Task.class);
            String taskName = taskAnnotation.value();
            taskMethods.put(taskName, method);
            String detail = getMethodDetail(method);
            taskMethodDetails.put(taskName, detail);
            taskBeans.put(taskName, getBean(method));
          } catch (Exception e) {
            throw new DTSException(e);
          }
        }
      }
    }
    return new TaskMethodWrapper(taskMethods, taskMethodDetails, taskBeans);
  }

  public List<Class> getClasses(String packageName) {
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      String path = packageName.replace('.', '/');
      Enumeration<URL> resources = classLoader.getResources(path);
      List<File> dirs = Lists.newArrayList();
      while (resources.hasMoreElements()) {
        URL resource = resources.nextElement();
        URI uri = new URI(resource.toString());
        dirs.add(new File(uri.getPath()));
      }
      List<Class> classes = Lists.newArrayList();
      for (File directory : dirs) {
        classes.addAll(findClasses(directory, packageName));
      }
      return classes;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
    List<Class> classes = Lists.newArrayList();
    if (!directory.exists()) {
      return classes;
    }
    File[] files = directory.listFiles();
    for (File file : files) {
      if (file.isDirectory()) {
        classes.addAll(findClasses(file, packageName + "." + file.getName()));
      } else if (file.getName().endsWith(".class")) {
        classes.add(Class.forName(packageName + "." + file.getName().substring(0, file.getName().length() - 6)));
      }
    }
    return classes;
  }

  private String getMethodDetail(Method method) {
    StringBuilder sb = new StringBuilder();
    sb.append(method.getDeclaringClass().getSimpleName()).append(".")
        .append(method.getName()).append("(");
    int count = 0;
    for (Parameter param : method.getParameters()) {
      if (++count > 1) {
        sb.append(",");
      }
      sb.append(param.getType().getSimpleName()).append(" ").append(param.getName());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.context = applicationContext;
  }

  public Object getBean(Method method) {
    return context.getBean(method.getDeclaringClass());
  }
}
