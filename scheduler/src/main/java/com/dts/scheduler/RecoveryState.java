package com.dts.scheduler;

/**
 * master节点状态，standby为备份节点，alive为leader节点
 *
 * @author zhangxin
 */
public enum RecoveryState {
  STANDBY, ALIVE
}
