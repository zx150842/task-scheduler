package com.dts.rpc.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Created by zhangxin on 2016/11/26.
 */
public interface Encodable {

  int encodeLength();

  void encode(ByteBuf buf);
}
