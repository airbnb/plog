package com.airbnb.plog.common

import com.google.common.hash.Hashing
import io.netty.buffer.Unpooled

class Murmur3Test extends GroovyTestCase {
    void testGuavaCompat() {
        final raw = 'abcdefghijklmnopqrstuvwxyz'.bytes

        for (seed in [0, 1, 10])
            for (len in 0..20) {
                final model = Arrays.copyOf(raw, len)
                final guavaHash = Hashing.murmur3_32(seed).hashBytes(model).asInt()
                final plogHash = Murmur3.hash32(Unpooled.wrappedBuffer(model), 0, len, seed)
                assert plogHash == guavaHash
            }
    }

    void testOffsetsAndLength() {
        final raw = 'abcdef'.bytes
        final bb = Unpooled.wrappedBuffer(raw)

        for (from in 0..raw.length)
            for (to in from..raw.length) {
                final model = Arrays.copyOfRange(raw, from, to)
                final guavaHash = Hashing.murmur3_32().hashBytes(model).asInt()
                final plogHash = Murmur3.hash32(bb, from, to - from, 0)
                assert plogHash == guavaHash
            }
    }
}
