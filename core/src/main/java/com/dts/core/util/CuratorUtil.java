package com.dts.core.util;

import com.dts.core.DTSConf;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author zhangxin
 */
public class CuratorUtil {

  private static final int ZK_CONNECTION_TIMEOUT_MILLIS = 15000;
  private static final int ZK_SESSION_TIMEOUT_MILLIS = 60000;
  private static final int RETRY_WAIT_MILLS = 5000;
  private static final int MAX_RECONNECT_ATTEMPTS = 3;

  public static CuratorFramework newClient(DTSConf conf) {
    String ZK_URL = conf.get("dts.master.zookeeper.url");
    CuratorFramework zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLS, MAX_RECONNECT_ATTEMPTS));
    zk.start();
    return zk;
  }

  public static void mkdir(CuratorFramework zk, String path) {
    try {
      if (zk.checkExists().forPath(path) == null) {
        zk.create().creatingParentsIfNeeded().forPath(path);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteRecursive(CuratorFramework zk, String path) {
    try {
      if (zk.checkExists().forPath(path) != null) {
        for (String child : zk.getChildren().forPath(path)) {
          zk.delete().forPath(path + "/" + child);
        }
        zk.delete().forPath(path);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
