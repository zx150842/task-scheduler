package com.dts.core.master;

import com.dts.core.util.CuratorUtil;
import com.dts.rpc.DTSConf;
import com.dts.rpc.util.SerializerInstance;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author zhangxin
 */
public class ZooKeeperPersistenceEngine {
  private final DTSConf conf;
  private final SerializerInstance serializer;
  private final String WORKING_DIR;
  private final CuratorFramework zk;

  public ZooKeeperPersistenceEngine(DTSConf conf, SerializerInstance serializer) {
    this.conf = conf;
    this.serializer = serializer;
    WORKING_DIR = conf.get("dts.master.zookeeper.dir", "/dts") + "/master_status";
    zk = CuratorUtil.newClient(conf);
    CuratorUtil.mkdir(zk, WORKING_DIR);
  }

  public void persist(String name, Object obj) {
    serializeIntoFile(WORKING_DIR + "/" + name, obj);
  }

  public void unpersist(String name) {
    try {
      zk.delete().forPath(WORKING_DIR + "/" + name);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T[] read(String prefix) {
    try {
      List<T> result = Lists.newArrayList();
      List<String> paths = zk.getChildren().forPath(WORKING_DIR);
      for (String path : paths) {
        if (path.startsWith(prefix)) {
          result.add(deserializeFromFile(path));
        }
      }
      return (T[]) result.toArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    zk.close();
  }

  private void serializeIntoFile(String path, Object value) {
    ByteBuffer serialized = serializer.serialize(value);
    byte[] bytes = new byte[serialized.remaining()];
    serialized.get(bytes);
    try {
      zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T deserializeFromFile(String fileName) {
    try {
      byte[] fileData = zk.getData().forPath(WORKING_DIR + "/" + fileName);
      return serializer.deserialize(ByteBuffer.wrap(fileData));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
