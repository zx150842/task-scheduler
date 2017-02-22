package com.dts.executor.example;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangxin
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class WorkerTest {

  @Test
  public void workerTest() throws InterruptedException {
    TimeUnit.SECONDS.sleep(100);
  }
}
