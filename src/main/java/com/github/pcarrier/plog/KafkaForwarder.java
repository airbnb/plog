package com.github.pcarrier.plog;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.io.IOException;
import java.util.regex.Pattern;

import static io.netty.channel.ChannelHandler.Sharable;

@Sharable
final class KafkaForwarder extends SimpleChannelInboundHandler {
    private final static String TOPIC = System.getProperty("plog.topic", "flog");

    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$",
            Pattern.CASE_INSENSITIVE
    );

    private Producer<String, String> producer;

    KafkaForwarder(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!(cause instanceof IOException && IGNORABLE_ERROR_MESSAGE.matcher(cause.getMessage()).matches()))
            super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        String s = (String) o;
        producer.send(new KeyedMessage<String, String>(TOPIC, s));
    }
}
