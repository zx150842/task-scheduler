package com.dts.executor.task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.dts.core.exception.DTSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipException;

/**
 * 任务扫描器，用来扫描应用程序中所有包含{@link Task}注解的方法
 *
 * @author zhangxin
 */
@Component
public class TaskScanner implements ApplicationContextAware {

  private final Logger logger = LoggerFactory.getLogger(TaskScanner.class);
  private ApplicationContext context;

  private static final String URL_PROTOCOL_JAR = "jar";
  private static final String URL_PROTOCOL_ZIP = "zip";
  private static final String JAR_URL_SEPARATOR = "!/";
  private static final String FILE_URL_PREFIX = "file:";
  private static final String CLASS_SUFFIX = ".class";

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
            if (taskMethods.containsKey(taskName)) {
              throw new IllegalArgumentException(String.format("Task name: {} has already exists in project, "
                + "please ensure task name is unique", taskName));
            }
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
      List<Class> classes = Lists.newArrayList();
      while (resources.hasMoreElements()) {
        URL resource = resources.nextElement();
        if (isJarURL(resource)) {
          List<Class> list = doFindJarClasses(resource);
          if (list != null && !list.isEmpty()) {
            classes.addAll(list);
          }
        } else {
          URI uri = new URI(resource.toString());
          List<Class> list = doFindFileClasses(new File(uri.getPath()), packageName);
          if (list != null && !list.isEmpty()) {
            classes.addAll(list);
          }
        }
      }
      return classes;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public List<Class> doFindFileClasses(File directory, String packageName) throws ClassNotFoundException {
    List<Class> classes = Lists.newArrayList();
    if (!directory.exists()) {
      return classes;
    }
    File[] files = directory.listFiles();
    for (File file : files) {
      if (file.isDirectory()) {
        classes.addAll(doFindFileClasses(file, packageName + "." + file.getName()));
      } else if (file.getName().endsWith(CLASS_SUFFIX)) {
        classes.add(Class.forName(packageName + "." + file.getName().substring(0, file.getName().length() - CLASS_SUFFIX.length())));
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

  private boolean isJarURL(URL url) {
    String protocol = url.getProtocol();
    return URL_PROTOCOL_JAR.equals(protocol) || URL_PROTOCOL_ZIP.equals(protocol);
  }

  private List<Class> doFindJarClasses(URL rootURL) throws IOException, ClassNotFoundException {
    URLConnection con = rootURL.openConnection();
    JarFile jarFile;
    String jarFileUrl;
    String rootEntryPath;
    boolean newJarFile = false;
    if (con instanceof JarURLConnection) {
      JarURLConnection jarCon = (JarURLConnection) con;
      jarFile = jarCon.getJarFile();
      jarFileUrl = jarCon.getJarFileURL().toExternalForm();
      JarEntry jarEntry = jarCon.getJarEntry();
      rootEntryPath = (jarEntry != null ? jarEntry.getName() : "");
    } else {
      String urlFile = rootURL.getFile();
      try {
        int separatorIndex = urlFile.indexOf(JAR_URL_SEPARATOR);
        if (separatorIndex != -1) {
          jarFileUrl = urlFile.substring(0, separatorIndex);
          rootEntryPath = urlFile.substring(separatorIndex + JAR_URL_SEPARATOR.length());
          jarFile = getJarFile(jarFileUrl);
        } else {
          jarFile = new JarFile(urlFile);
          jarFileUrl = urlFile;
          rootEntryPath = "";
        }
        newJarFile = true;
      } catch (ZipException ex) {
        logger.warn("Skipping invalid jar classpath entry {}", urlFile);
        return Collections.emptyList();
      }
    }

    try {
      logger.info("Looking for matching resources in jar file {}", jarFileUrl);
      if ("".equals(rootEntryPath) && !rootEntryPath.endsWith("/")) {
        rootEntryPath = rootEntryPath + "/";
      }
      Set<Class> classes = Sets.newLinkedHashSet();
      for (Enumeration<JarEntry> entries = jarFile.entries(); entries.hasMoreElements();) {
        JarEntry entry = entries.nextElement();
        String entryPath = entry.getName();
        if (entryPath.startsWith(rootEntryPath) && entry.getName().contains(CLASS_SUFFIX)) {
          String classPath = entry.getName().substring(0, entry.getName().length() - CLASS_SUFFIX.length());
          classes.add(Class.forName(classPath.replace("/", ".")));
        }
      }
      return Lists.newArrayList(classes);
    } finally {
      if (newJarFile) {
        jarFile.close();
      }
    }
  }

  private JarFile getJarFile(String jarFileUrl) throws IOException {
    if (jarFileUrl.startsWith(FILE_URL_PREFIX)) {
      try {
        return new JarFile(new URI(StringUtils.replace(jarFileUrl, " ", "%20")).getSchemeSpecificPart());
      } catch (URISyntaxException e) {
        return new JarFile(jarFileUrl.substring(FILE_URL_PREFIX.length()));
      }
    } else {
      return new JarFile(jarFileUrl);
    }
  }
}
