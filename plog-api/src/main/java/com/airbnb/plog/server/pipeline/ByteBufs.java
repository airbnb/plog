package com.airbnb.plog.server.pipeline;

import io.netty.buffer.ByteBuf;

public final class ByteBufs {
    public static byte[] toByteArray(ByteBuf buf) {
        final byte[] payload = new byte[buf.readableBytes()];
        buf.getBytes(0, payload);
        return payload;
    }
}
