package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import static io.netty.channel.ChannelHandler.Sharable;

@RequiredArgsConstructor
@Sharable
final class KafkaForwarder extends SimpleChannelInboundHandler<ByteBuf> {
    // This makes me excrutiatingly sad
    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$",
            Pattern.CASE_INSENSITIVE
    );

    private final String topic;
    private final Producer<String, String> producer;
    private final StatisticsReporter stats;
    private final Charset charset;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!(cause instanceof IOException && IGNORABLE_ERROR_MESSAGE.matcher(cause.getMessage()).matches()))
            super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        final int length = msg.readableBytes();
        final byte[] bytes = new byte[length];
        msg.readBytes(bytes);
        String str = new String(bytes, charset);

        try {
            producer.send(new KeyedMessage<String, String>(topic, str));
        } catch (FailedToSendMessageException e) {
            stats.failedToSend();
        }
    }
}
