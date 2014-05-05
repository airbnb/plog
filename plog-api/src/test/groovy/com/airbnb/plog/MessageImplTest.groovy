package com.airbnb.plog

import io.netty.buffer.ByteBufAllocator

class MessageImplTest extends GroovyTestCase {
    final payload = 'foo'.bytes
    final tags = ['bar', 'baz']
    final simpleMsg = MessageImpl.fromBytes(ByteBufAllocator.DEFAULT, payload, null)
    final taggedMsg = MessageImpl.fromBytes(ByteBufAllocator.DEFAULT, payload, tags)

    void testRead() {
        assert simpleMsg.asBytes() == payload
        assert taggedMsg.asBytes() == payload
    }

    void testToString() {
        assert simpleMsg.toString() == 'foo'
        assert taggedMsg.toString() == '[bar,baz] foo'
    }
}
