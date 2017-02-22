package com.dts.executor.example.config;

import com.dts.core.DTSConf;
import com.dts.executor.WorkerLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Iterator;

/**
 * @author zhangxin
 */
@Component
@org.springframework.context.annotation.PropertySource(
  "classpath:application-${spring.profiles.active}.properties")
public class ExecutorConfig extends WorkerLauncher {
  @Autowired
  private Environment env;

  @Override
  protected DTSConf getConfig() {
    DTSConf conf = new DTSConf(false);
    for (Iterator it = ((AbstractEnvironment)env).getPropertySources().iterator(); it.hasNext();) {
      PropertySource propertySource = (PropertySource) it.next();
      if (propertySource instanceof MapPropertySource) {
        MapPropertySource mapPropertySource = (MapPropertySource) propertySource;
        for (String key : mapPropertySource.getSource().keySet()) {
          if (key.startsWith("dts")) {
            conf.set(key, (String) mapPropertySource.getSource().get(key));
          }
        }
      }
    }
    return conf;
  }
}
