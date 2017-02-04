package com.dts.core.rpc.network.protocol;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

/**
 * Created by zhangxin on 2016/11/27.
 */
public class Encoders {

  public static class Strings {
    public static String decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }

    public static int encodedLength(String s) {
      return 4 + s.getBytes(StandardCharsets.UTF_8).length;
    }

    public static void encode(ByteBuf buf, String s) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
    }
  }
}
