package com.airbnb.plog.utils;

import io.netty.buffer.ByteBuf;

public final class ByteBufs {
    public static byte[] toByteArray(ByteBuf buf) {
        final int length = buf.readableBytes();
        final byte[] payload = new byte[length];
        buf.getBytes(0, payload, 0, length);
        return payload;
    }
}
