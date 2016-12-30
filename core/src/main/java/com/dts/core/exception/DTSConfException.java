package com.dts.core.exception;

/**
 * @author zhangxin
 */
public class DTSConfException extends RuntimeException {

  public DTSConfException() {
    super();
  }

  public DTSConfException(String message) {
    super(message);
  }

  public DTSConfException(String message, Throwable cause) {
    super(message, cause);
  }

  public DTSConfException(Throwable cause) {
    super(cause);
  }
}
