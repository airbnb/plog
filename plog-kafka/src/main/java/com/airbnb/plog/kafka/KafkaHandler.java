package com.airbnb.plog.kafka;

import com.airbnb.plog.Message;
import com.airbnb.plog.handlers.Handler;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.yammer.metrics.core.Meter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
@Slf4j
public class KafkaHandler extends SimpleChannelInboundHandler<Message> implements Handler {
    private final String defaultTopic;
    private final Producer<byte[], byte[]> producer;
    private final AtomicLong failedToSendMessageExceptions = new AtomicLong(), seenMessages = new AtomicLong();
    private final ProducerStats producerStats;
    private final ProducerTopicMetrics producerAllTopicsStats;

    protected KafkaHandler(final String clientId, final String defaultTopic, final Producer<byte[], byte[]> producer) {
        super();
        this.producerStats = ProducerStatsRegistry.getProducerStats(clientId);
        this.producerAllTopicsStats = ProducerTopicStatsRegistry.getProducerTopicStats(clientId).getProducerAllTopicsStats();
        this.defaultTopic = defaultTopic;
        this.producer = producer;
    }

    private static JsonObject meterToJsonObject(Meter meter) {
        return new JsonObject()
                .add("count", meter.count())
                .add("rate", new JsonArray()
                        .add(meter.oneMinuteRate())
                        .add(meter.fiveMinuteRate())
                        .add(meter.fifteenMinuteRate()));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        seenMessages.incrementAndGet();
        final byte[] payload = msg.asBytes();

        boolean sawKtTag = false;

        for (String tag : msg.getTags())
            if (tag.startsWith("kt:")) {
                sawKtTag = true;
                sendOrReportFailure(tag.substring(3), payload);
            }

        if (!sawKtTag)
            sendOrReportFailure(defaultTopic, payload);
    }

    private boolean sendOrReportFailure(String topic, byte[] msg) {
        final boolean nonNullTopic = !("null".equals(topic));
        if (nonNullTopic)
            try {
                producer.send(new KeyedMessage<byte[], byte[]>(topic, msg));
            } catch (FailedToSendMessageException e) {
                log.warn("Failed to send to topic {}", topic, e);
                failedToSendMessageExceptions.incrementAndGet();
            }
        return nonNullTopic;
    }

    @Override
    public JsonObject getStats() {
        return new JsonObject()
                .add("default_topic", defaultTopic)
                .add("seen_messages", seenMessages.get())
                .add("failed_to_send", failedToSendMessageExceptions.get())
                .add("failed_send_rate", meterToJsonObject(producerStats.failedSendRate()))
                .add("resend_rate", meterToJsonObject(producerStats.resendRate()))
                .add("serialization_error_rate", meterToJsonObject(producerStats.serializationErrorRate()))
                .add("message_rate", meterToJsonObject(producerAllTopicsStats.messageRate()))
                .add("dropped_message_rate", meterToJsonObject(producerAllTopicsStats.droppedMessageRate()))
                .add("byte_rate", meterToJsonObject(producerAllTopicsStats.byteRate()));
    }

    @Override
    public final String getName() {
        return "kafka";
    }
}
