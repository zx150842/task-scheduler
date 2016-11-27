package com.dts.rpc.network.protocol;

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
    }
}
