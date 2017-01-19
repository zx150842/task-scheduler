package com.dts.rpc.exception;

/**
 * @author zhangxin
 */
public class DTSException extends RuntimeException {
  public DTSException(String message) {
    super(message);
  }

  public DTSException(String message, Throwable cause) {
    super(message, cause);
  }

  public DTSException(Throwable cause) {
    super(cause);
  }
}
