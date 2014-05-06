package com.airbnb.plog.client.fragmentation

import com.airbnb.plog.Message
import com.airbnb.plog.server.fragmentation.Defragmenter
import com.airbnb.plog.server.pipeline.ProtocolDecoder
import com.airbnb.plog.server.stats.SimpleStatisticsReporter
import com.typesafe.config.ConfigFactory
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.socket.DatagramPacket
import org.junit.Test

class FragmenterTest extends GroovyTestCase {
    public static final InetSocketAddress socket = new InetSocketAddress('localhost', 2000)

    // 208 bytes
    static final BYTES = ((('a'..'z') + ('A'..'Z')).join() * 4).bytes

    final statsReporter = new SimpleStatisticsReporter()
    final defragConfig = ConfigFactory.defaultReference().getConfig('plog.server.udp.defaults.defrag')
    final serverChannel = new EmbeddedChannel(
            new ProtocolDecoder(statsReporter),
            new Defragmenter(statsReporter, defragConfig))

    @Test
    public void testSimple() throws Exception {
        int i = 0
        for (fragmentSize in [25, 40, 60]) {
            final fragmenter = new Fragmenter(fragmentSize)
            for (payloadLength in (0..5).asList() + [20, 50, 100, 200]) {
                for (tags in [null, [], ['foo'], ['foo', 'bar']]) {
                    if (fragmentSize == 25 && tags != null && !tags.isEmpty())
                        // no room :(
                        break

                    final payload = BYTES[0..payloadLength].toArray() as byte[]
                    final fragments = fragmenter.fragment(ByteBufAllocator.DEFAULT, payload, tags, 0)

                    for (fragment in fragments) {
                        serverChannel.writeInbound(new DatagramPacket(fragment, socket, socket))
                    }

                    final msg = (Message) serverChannel.readInbound()

                    assert msg.asBytes() == payload

                    if (tags == null)
                        assert msg.tags == []
                    else
                        assert msg.tags == tags

                    i++
                }
            }
        }
    }
}
