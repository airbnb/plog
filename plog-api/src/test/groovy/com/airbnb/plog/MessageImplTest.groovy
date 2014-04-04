package com.airbnb.plog

import io.netty.buffer.ByteBufAllocator

class MessageImplTest extends GroovyTestCase {
    void testCreateAndRead() {
        def content = 'foo'.bytes
        assert MessageImpl.fromBytes(ByteBufAllocator.DEFAULT, content).asBytes() == content
    }
}
