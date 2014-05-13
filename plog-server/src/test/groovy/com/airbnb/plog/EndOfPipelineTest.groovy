package com.airbnb.plog

import com.airbnb.plog.server.pipeline.EndOfPipeline
import com.airbnb.plog.server.stats.SimpleStatisticsReporter
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.embedded.EmbeddedChannel

class EndOfPipelineTest extends GroovyTestCase {
    void testExceptionCaughtInPipeline() {
        final oldErr = System.err
        final logged = new ByteArrayOutputStream()
        System.setErr(new PrintStream(logged))

        final thrower = new SimpleChannelInboundHandler() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
                throw new Exception()
            }
        }

        final stats = new SimpleStatisticsReporter()

        final pipeline = new EmbeddedChannel(thrower, new EndOfPipeline(stats))

        assert logged.toString().readLines().isEmpty()
        assert stats.exception() == 1

        assert !pipeline.writeInbound(new Object())

        assert stats.exception() == 3
        assert logged.toString().contains('Exception down the pipeline')

        System.setErr(oldErr)
    }
}
