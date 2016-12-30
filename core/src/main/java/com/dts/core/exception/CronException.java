package com.dts.core.exception;

/**
 * @author zhangxin
 */
public class CronException extends RuntimeException {

  public CronException() {
    super();
  }

  public CronException(String message) {
    super(message);
  }

  public CronException(String message, Throwable cause) {
    super(message, cause);
  }

  public CronException(Throwable cause) {
    super(cause);
  }
}
