package com.dts.core.exception;

/**
 * @author zhangxin
 */
public class DTSSerializeException extends RuntimeException {

  public DTSSerializeException(String message) {
    super(message);
  }

  public DTSSerializeException(String message, Throwable cause) {
    super(message, cause);
  }

  public DTSSerializeException(Throwable cause) {
    super(cause);
  }
}
