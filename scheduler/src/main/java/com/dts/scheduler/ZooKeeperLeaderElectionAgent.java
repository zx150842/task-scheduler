package com.dts.scheduler;

import com.dts.core.DTSConf;
import com.dts.core.exception.DTSException;
import com.dts.core.util.CuratorUtil;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 用来在多个master之间选择leader节点
 *
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
    WORKING_DIR = conf.get("dts.zookeeper.dir", "/dts") + "/leader_election";
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
      throw new DTSException(e);
    }
  }

  public void stop() {
    try {
      leaderLatch.close();
      zk.close();
    } catch (IOException e) {
      throw new DTSException(e);
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
      master.electedLeader();
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER;
      master.revokedLeadership();
    }
  }

  enum LeadershipStatus {
    LEADER, NOT_LEADER
  }
}
