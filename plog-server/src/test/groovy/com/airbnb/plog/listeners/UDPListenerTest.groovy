package com.airbnb.plog.listeners

import com.airbnb.plog.handlers.MessageQueueProvider
import com.typesafe.config.ConfigFactory

class UDPListenerTest extends GroovyTestCase {
    final static LOOPBACK_ADDR = Inet4Address.getLoopbackAddress()
    public static final int PORT = 23456

    final refConfig = ConfigFactory.defaultReference().getConfig('plog')
    final defaultUDPConfig = refConfig.getConfig('udp.defaults')
            .withFallback(refConfig.getConfig('defaults'))

    private void runTest(Map config, Closure test, byte[] expectation) {
        final compiledConfig = ConfigFactory.parseMap(config).withFallback(defaultUDPConfig)
        final listener = new UDPListener(compiledConfig)
        listener.start().await()

        test.run()

        final start = System.currentTimeMillis()

        def grabbed = null
        while (System.currentTimeMillis() - start < 5000 && grabbed == null) {
            Thread.sleep(100)
            grabbed = MessageQueueProvider.queue.poll()
        }

        assert grabbed != null
        assert grabbed.asBytes() == expectation
        grabbed.release()

        listener.group.shutdownGracefully().await()
    }

    void testMultipleHandlers() {
        final config = [handlers: [[provider: 'com.airbnb.plog.handlers.ReverseBytesProvider'],
                                   [provider: 'com.airbnb.plog.handlers.TruncationProvider', max_length: 5],
                                   [provider: 'com.airbnb.plog.handlers.MessageQueueProvider']]
        ]
        runTest(config, {
            final socket = new DatagramSocket()
            sendPacket(socket, 'hello world'.bytes)
            socket.close()
        }, 'dlrow'.bytes)
    }

    void testSingleFragment() {
        final config = [handlers: [[provider: 'com.airbnb.plog.handlers.MessageQueueProvider']]]
        final fragment = [
                0, // version
                1, // type
                0, 1, // fragment count
                0, 0, // fragment index
                0, 5, // fragment length
                0, 0, 0, 0, // identifier
                0, 0, 0, 5, // message length
                0x24, 0x8b, 0xfa, 0x47, // checksum
                0, 0, 0, 0, // zeroes
                104, 101, 108, 108, 111] as byte[]

        runTest(config, {
            final socket = new DatagramSocket()
            sendPacket(socket, fragment)
            socket.close()
        }, 'hello'.bytes)
    }

    void testMultiFragment() {
        final config = [handlers: [[provider: 'com.airbnb.plog.handlers.MessageQueueProvider']]]
        final fragment1 = [
                0, // version
                1, // type
                0, 2, // fragment count
                0, 0, // fragment index
                0, 3, // fragment length
                0, 0, 0, 0, // identifier
                0, 0, 0, 5, // message length
                0x24, 0x8b, 0xfa, 0x47, // checksum
                0, 0, 0, 0, // zeroes
                104, 101, 108] as byte[]

        final fragment2 = [
                0, // version
                1, // type
                0, 2, // fragment count
                0, 1, // fragment index
                0, 3, // fragment length
                0, 0, 0, 0, // identifier
                0, 0, 0, 5, // message length
                0x24, 0x8b, 0xfa, 0x47, // checksum
                0, 0, 0, 0, // zeroes
                108, 111] as byte[]

        runTest(config, {
            final socket = new DatagramSocket()
            sendPacket(socket, fragment1)
            sendPacket(socket, fragment2)
            socket.close()
        }, 'hello'.bytes)
    }

    static void sendPacket(DatagramSocket socket, byte[] payload) {
        final packet = new DatagramPacket(payload, payload.length, LOOPBACK_ADDR, PORT)
        Thread.sleep(100)
        socket.send(packet)
    }
}
