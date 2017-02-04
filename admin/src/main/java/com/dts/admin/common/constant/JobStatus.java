package com.dts.admin.common.constant;

/**
 * @author zhangxin
 */
public enum JobStatus {
  DELETE(-1), INIT(0), VALID(1), PAUSE(2);
  public final int status;

  JobStatus(int status) {
    this.status = status;
  }
}
