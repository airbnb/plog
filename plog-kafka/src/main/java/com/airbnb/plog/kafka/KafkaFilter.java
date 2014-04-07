package com.airbnb.plog.kafka;

import com.airbnb.plog.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KafkaFilter extends SimpleChannelInboundHandler<Message> {
    private final Producer<byte[], byte[]> producer;
    private final String defaultTopic;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        try {
            producer.send(new KeyedMessage<byte[], byte[]>(defaultTopic, msg.asBytes()));
        } catch (FailedToSendMessageException e) {
            // TODO(pierre): reintroduce stats
            // stats.failedToSend();
        }
    }
}
