package com.dts.rpc.network.protocol;

import com.dts.rpc.network.buffer.ManagedBuffer;

/**
 * @author zhangxin
 */
public interface Message {

  ManagedBuffer body();
}
