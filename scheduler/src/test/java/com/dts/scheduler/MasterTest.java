package com.dts.scheduler;

import com.dts.core.DTSConf;
import com.dts.core.util.DTSConfUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
public class MasterTest {

  private Master _master;

  @Before
  public void setup() {
    DTSConf conf = DTSConfUtil.readFile("dts.properties");
    _master = Master.launchMaster(conf);
  }

  @After
  public void tearDown() {
    _master.stop();
  }

  @Test
  public void testElectedLeader() throws InterruptedException {
    TimeUnit.SECONDS.sleep(360000);
  }

  @Test
  public void testSendTask() {
    _master.schedule();
  }

  @Test
  public void test() {
    boolean runOnSeed = false;
    boolean isSeed = false;
    System.out.println(!(isSeed ^ runOnSeed));
  }


}
