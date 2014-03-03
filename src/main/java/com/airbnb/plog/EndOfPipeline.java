package com.airbnb.plog;

import com.airbnb.plog.stats.StatisticsReporter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.regex.Pattern;

@ChannelHandler.Sharable
@Slf4j
@RequiredArgsConstructor
public class EndOfPipeline extends SimpleChannelInboundHandler<Void> {
    private final StatisticsReporter stats;

    // This makes me excrutiatingly sad
    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Void msg) throws Exception {
        log.error("Some seriously weird stuff going on here!");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!(cause instanceof IOException && IGNORABLE_ERROR_MESSAGE.matcher(cause.getMessage()).matches())) {
            log.error("Exception down the pipeline", cause);
            stats.exception();
        }
    }
}