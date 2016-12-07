package com.dts.core.master;

import com.dts.core.util.CuratorUtil;
import com.dts.rpc.DTSConf;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author zhangxin
 */
public class ZooKeeperLeaderElectionAgent implements LeaderLatchListener {
  private final Logger logger = LoggerFactory.getLogger(ZooKeeperLeaderElectionAgent.class);

  private final Master master;
  private final DTSConf conf;
  private final String WORKING_DIR;

  private CuratorFramework zk;
  private LeaderLatch leaderLatch;
  private LeadershipStatus status = LeadershipStatus.NOT_LEADER;

  public ZooKeeperLeaderElectionAgent(Master master, DTSConf conf) {
    this.master = master;
    this.conf = conf;
    WORKING_DIR = conf.get("dts.master.zookeeper.dir", "/dts") + "/leader_election";
    start();
  }

  private void start() {
    logger.info("Starting ZooKeeper LeaderElection agent");
    zk = CuratorUtil.newClient(conf);
    leaderLatch = new LeaderLatch(zk, WORKING_DIR);
    leaderLatch.addListener(this);
    try {
      leaderLatch.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    try {
      leaderLatch.close();
      zk.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void isLeader() {
    synchronized (this) {
      if (!leaderLatch.hasLeadership()) {
        return;
      }
      logger.info("We have gained leadership");
      updateLeadershipStatus(true);
    }
  }

  @Override
  public void notLeader() {
    synchronized (this) {
      if (leaderLatch.hasLeadership()) {
        return;
      }
      logger.info("We have lost leadership");
      updateLeadershipStatus(false);
    }
  }

  private void updateLeadershipStatus(boolean isLeader) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER;

    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER;
    }
  }

  enum LeadershipStatus {
    LEADER, NOT_LEADER
  }
}