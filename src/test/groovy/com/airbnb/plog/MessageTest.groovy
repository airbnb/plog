package com.airbnb.plog

import io.netty.buffer.ByteBufAllocator

class MessageTest extends GroovyTestCase {
    void testCreateAndRead() {
        def content = 'foo'.bytes
        assert Message.fromBytes(ByteBufAllocator.DEFAULT, content).asBytes() == content
    }
}
