package com.airbnb.plog.utils

import io.netty.buffer.Unpooled

class ByteBufsTest extends GroovyTestCase {
    final static byte[][] INTERESTING_BYTE_ARRAYS = [
            ''.bytes,             // empty
            'foo'.bytes,          // short
            ('foo' * 1000).bytes, // long
            (0..255).toArray()    // binary with \0
    ]

    void testToByteArray() {
        for (testCase in INTERESTING_BYTE_ARRAYS) {
            final buf = Unpooled.wrappedBuffer(testCase)
            assert ByteBufs.toByteArray(buf) == testCase
        }
    }
}
