package com.airbnb.plog

class MessageTest extends GroovyTestCase {
    void testCreateAndRead() {
        final payload = 'foo'.bytes
        assert new Message(payload).payload == payload
    }
}
