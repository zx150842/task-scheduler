package com.dts.core.master;

/**
 * @author zhangxin
 */
abstract class MasterMessages {
}

class ElectedLeader extends MasterMessages{}

class RevokedLeadership extends MasterMessages{}

class SyncZKWorkers extends MasterMessages{}

class BeginRecovery extends MasterMessages{}

class CompleteRecovery extends MasterMessages{}

class BoundPortsRequest extends MasterMessages{}

class BoundPortsResponse extends MasterMessages{}
