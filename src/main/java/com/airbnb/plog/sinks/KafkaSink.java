package com.airbnb.plog.sinks;

import com.airbnb.plog.Message;
import com.airbnb.plog.stats.StatisticsReporter;
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
public final class KafkaSink extends Sink {
    private final String topic;
    private final Producer<byte[], byte[]> producer;
    private final StatisticsReporter stats;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        try {
            producer.send(new KeyedMessage<byte[], byte[]>(topic, msg.getPayload()));
        } catch (FailedToSendMessageException e) {
            stats.failedToSend();
        }
    }
}
