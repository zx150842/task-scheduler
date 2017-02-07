package com.dts.executor.example;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:applicationContext.xml")
public class WorkerTest {

  @Test
  public void WorkerTest() throws InterruptedException {
    TimeUnit.SECONDS.sleep(100);
  }
}
