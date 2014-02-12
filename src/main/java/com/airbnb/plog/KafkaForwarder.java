package com.airbnb.plog;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.regex.Pattern;

import static io.netty.channel.ChannelHandler.Sharable;

@RequiredArgsConstructor
@Sharable
final class KafkaForwarder extends SimpleChannelInboundHandler<Message> {
    // This makes me excrutiatingly sad
    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$",
            Pattern.CASE_INSENSITIVE
    );

    private final String topic;
    private final Producer<byte[], byte[]> producer;
    private final StatisticsReporter stats;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!(cause instanceof IOException && IGNORABLE_ERROR_MESSAGE.matcher(cause.getMessage()).matches()))
            super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        try {
            producer.send(new KeyedMessage<byte[], byte[]>(topic, msg.getPayload()));
        } catch (FailedToSendMessageException e) {
            stats.failedToSend();
        }
    }
}
