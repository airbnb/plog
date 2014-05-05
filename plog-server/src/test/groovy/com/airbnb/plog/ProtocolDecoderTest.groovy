package com.airbnb.plog

import com.airbnb.plog.commands.FourLetterCommand
import com.airbnb.plog.fragmentation.Fragment
import com.airbnb.plog.stats.SimpleStatisticsReporter
import com.airbnb.plog.stats.StatisticsReporter
import com.airbnb.plog.utils.ByteBufs
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel

class ProtocolDecoderTest extends GroovyTestCase {
    void testForwardsUnboxed() {
        final firstPrintable = ' '.bytes
        final startsWitLastASCIIandNulInside = ((255..0) * 3).toArray() as byte[]
        for (payload in [firstPrintable, startsWitLastASCIIandNulInside]) {
            runTest { EmbeddedChannel channel, StatisticsReporter stats ->
                insert(payload, channel)
                final msg = (MessageImpl) channel.readInbound()
                assert msg.asBytes() == payload
            }
        }
    }

    void testReportsUnknownVersion() {
        runTest { EmbeddedChannel channel, StatisticsReporter stats ->
            for (version in 1..31)
                insert([version] as byte[], channel)
            assert stats.receivedUdpInvalidVersion() == 32
        }
    }

    void testReportsUnknownV0InvalidType() {
        runTest { EmbeddedChannel channel, StatisticsReporter stats ->
            for (type in 2..255)
                insert([0, type] as byte[], channel)
            assert stats.receivedV0InvalidType() == 255
        }
    }

    void testForwardsCommandsWithEmptyTrail() {
        runTest { EmbeddedChannel channel, StatisticsReporter stats ->
            insert('\0\0borg'.bytes, channel)
            final cmd = (FourLetterCommand) channel.readInbound()
            assert cmd.command == 'BORG'
            assert cmd.trail == []
        }
    }

    void testReportsCommandsTooShort() {
        runTest { EmbeddedChannel channel, StatisticsReporter stats ->
            insert('\0\0yo'.bytes, channel)
            assert stats.receivedUnknownCommand() == 2
        }
    }

    void testForwardsCommandsWithTrail() {
        runTest { EmbeddedChannel channel, StatisticsReporter stats ->
            insert('\0\0assimilation'.bytes, channel)
            final cmd = (FourLetterCommand) channel.readInbound()
            assert cmd.command == 'ASSI'
            assert cmd.trail == 'milation'.bytes
        }
    }

    void testForwardsFragments() {
        runTest { EmbeddedChannel channel, StatisticsReporter stats ->
            // I'm so sad fragment count and index are in this order
            final payload = (0..1) + (5..2) + (6..19) + [0, 0, 0, 0] + (24 .. 30)
            insert(payload as byte[], channel)
            final frag = (Fragment) channel.readInbound()
            assert frag.fragmentCount == (5 << 8) + 4
            assert frag.fragmentIndex == (3 << 8) + 2
            assert frag.fragmentSize == (6 << 8) + 7
            assert frag.msgId == ((Utils.clientAddr.port as long) << 32) +
                    (8 << 24) + (9 << 16) + (10 << 8) + 11
            assert frag.totalLength == (12 << 24) + (13 << 16) + (14 << 8) + 15
            assert frag.msgHash == (16 << 24) + (17 << 16) + (18 << 8) + 19
            assert ByteBufs.toByteArray(frag.content()) == 24..30
        }
    }

    private void runTest(Closure test) {
        final stats = new SimpleStatisticsReporter()
        final channel = new EmbeddedChannel(new ProtocolDecoder(stats))
        test.call(channel, stats)
        assert !channel.finish()
    }

    private void insert(byte[] payload, EmbeddedChannel channel) {
        final inserted = new io.netty.channel.socket.DatagramPacket(
                Unpooled.wrappedBuffer(payload),
                Utils.localAddr,
                Utils.clientAddr)
        channel.writeInbound(inserted)
    }
}
