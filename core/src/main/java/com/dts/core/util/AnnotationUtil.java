package com.dts.core.util;

import com.google.common.collect.Lists;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * @author zhangxin
 */
public class AnnotationUtil {

  public static List<Class> getClasses(String packageName) {
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

  public static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
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

  public static List<Method> getMethods(String packageName, Class<? extends Annotation> annotationClass) {
    List<Class> classes = getClasses(packageName);
    if (classes == null || classes.isEmpty()) {
      return Collections.emptyList();
    }
    List<Method> methods = Lists.newArrayList();
    for (Class clazz : classes) {
      for (Method method : clazz.getMethods()) {
        if (method.isAnnotationPresent(annotationClass)) {
          methods.add(method);
        }
      }
    }
    return methods;
  }
}
