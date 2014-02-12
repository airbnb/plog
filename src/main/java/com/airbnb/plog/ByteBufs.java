package com.airbnb.plog;

import io.netty.buffer.ByteBuf;

public class ByteBufs {
    static final byte[] toByteArray(ByteBuf buf) {
        final int length = buf.readableBytes();
        final byte[] payload = new byte[length];
        buf.getBytes(0, payload, 0, length);
        return payload;
    }
}
