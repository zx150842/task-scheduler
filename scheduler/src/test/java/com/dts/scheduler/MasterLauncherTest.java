package com.dts.scheduler;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class MasterLauncherTest {

  @Test
  public void test() throws InterruptedException {
    MasterLauncher launcher = new MasterLauncher();
    TimeUnit.SECONDS.sleep(10);
    System.exit(0);
  }
}
