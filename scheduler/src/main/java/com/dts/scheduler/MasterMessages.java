package com.dts.scheduler;

/**
 * @author zhangxin
 */
abstract class MasterMessages {
}

class ElectedLeader extends MasterMessages{}

class RevokedLeadership extends MasterMessages{}

class SyncZKWorkers extends MasterMessages{}
