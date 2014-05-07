package com.airbnb.plog.common;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteOrder;

@Slf4j
public class Murmur3 {
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    public static int hash32(ByteBuf data) {
        return hash32(data, data.readerIndex(), data.readableBytes(), 0);
    }

    public static int hash32(ByteBuf data, final int offset, final int length) {
        return hash32(data, offset, length, 0);
    }

    public static int hash32(ByteBuf data, final int offset, final int length, final int seed) {
        final ByteBuf ordered = data.order(ByteOrder.LITTLE_ENDIAN);

        int h = seed;

        final int len4 = length >>> 2;
        final int end4 = offset + (len4 << 2);

        for (int i = offset; i < end4; i += 4) {
            int k = ordered.getInt(i);

            k *= C1;
            k = k << 15 | k >>> 17;
            k *= C2;

            h ^= k;
            h = h << 13 | h >>> 19;
            h = h * 5 + 0xe6546b64;
        }

        int k = 0;
        switch (length & 3) {
            case 3:
                k = ordered.getByte(end4 + 2) << 16;
            case 2:
                k |= ordered.getByte(end4 + 1) << 8;
            case 1:
                k |= ordered.getByte(end4);

                k *= C1;
                k = (k << 15) | (k >>> 17);
                k *= C2;
                h ^= k;
        }

        h ^= length;
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;

        return h;
    }
}
