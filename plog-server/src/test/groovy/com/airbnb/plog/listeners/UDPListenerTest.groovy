package com.airbnb.plog.listeners
import com.typesafe.config.ConfigFactory

class UDPListenerTest extends GroovyTestCase {
    final static LOOPBACK_ADDR = Inet4Address.localHost
    public static final int PORT = 23456

    final refConfig = ConfigFactory.defaultReference().getConfig('plog')
    final defaultUDPConfig = refConfig.getConfig('udp.defaults')
            .withFallback(refConfig.getConfig('defaults'))

    private void runTest(Map config, Closure test, String expectation) {
        final compiledConfig = ConfigFactory.parseMap(config).withFallback(defaultUDPConfig)
        final listener = new UDPListener(compiledConfig)
        listener.start()

        final oldOut = System.out
        final newOut = new ByteArrayOutputStream()
        System.setOut(new PrintStream(newOut, true))

        Thread.sleep(100)

        test.run()

        final start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < 5000) {
            if (newOut.size() >= expectation.length())
                break
            else
                Thread.sleep(100)
        }

        final output = newOut.toString()
        assert output == expectation

        listener.group.shutdownGracefully().await()
        System.setOut(new PrintStream(oldOut))
    }

    void testMultipleHandlers() {
        final config = [handlers: [[provider: 'com.airbnb.plog.handlers.ReverseBytesProvider'],
                                   [provider: 'com.airbnb.plog.handlers.TruncationProvider', max_length: 5],
                                   [provider: 'com.airbnb.plog.console.ConsoleOutputProvider']]
        ]
        runTest(config, { sendPacket('hello world'.bytes) }, 'dlrow\n')
    }

    void testSingleFragment() {
        final config = [handlers: [[provider: 'com.airbnb.plog.console.ConsoleOutputProvider']]]
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
            Thread.sleep(10)
            sendPacket(fragment)
        }, 'hello\n')
    }

    void testMultiFragment() {
        final config = [handlers: [[provider: 'com.airbnb.plog.console.ConsoleOutputProvider']]]
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
            sendPacket(fragment1)
            sendPacket(fragment2)
        }, 'hello\n')
    }

    void sendPacket(byte[] payload) {
        final socket = new DatagramSocket()
        socket.send(new DatagramPacket(payload, payload.length, LOOPBACK_ADDR, PORT))
        socket.close()
    }
}
