package com.dts.core.util;

import com.dts.core.exception.CronException;
import com.google.common.base.Preconditions;

import java.text.ParseException;
import java.util.Date;

/**
 * @author zhangxin
 */
public class CronExpressionUtil {

  public static Date getNextTriggerTime(String cronExpression) {
    return getNextTriggerTime(cronExpression, new Date());
  }

  public static Date getNextTriggerTime(String cronExpression, Date timeAfter) {
    Preconditions.checkNotNull(timeAfter);
    try {
      CronExpression cron = new CronExpression(cronExpression);
      return cron.getTimeAfter(timeAfter);
    } catch (ParseException e) {
      throw new CronException(e);
    }
  }

  public static boolean isValidExpression(String cronExpression) {
    return CronExpression.isValidExpression(cronExpression);
  }
}
