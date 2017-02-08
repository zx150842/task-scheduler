package com.dts.admin.service;

import com.dts.admin.Application;
import com.dts.admin.common.dto.JobDto;
import com.dts.admin.common.vo.CronJob;
import com.dts.admin.dao.CronJobDao;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

/**
 * @author zhangxin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Application.class)
public class CronJobServiceTest {
  @Autowired
  private CronJobDao cronJobDao;

  @Test
  public void test() {
    List<CronJob> list =  cronJobDao.getAll(null, null);
    for (CronJob jobDto : list) {
      System.out.println(jobDto.getLastTriggerTime());
    }
  }
}
