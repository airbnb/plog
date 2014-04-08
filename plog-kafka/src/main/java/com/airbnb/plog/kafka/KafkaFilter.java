package com.airbnb.plog.kafka;

import com.airbnb.plog.Message;
import com.airbnb.plog.filters.Filter;
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
public class KafkaFilter extends SimpleChannelInboundHandler<Message> implements Filter {
    private final String defaultTopic;
    private final Producer<byte[], byte[]> producer;
    private final AtomicLong failedToSendMessageExceptions = new AtomicLong();
    private final ProducerStats producerStats;
    private final ProducerTopicMetrics producerAllTopicsStats;

    protected KafkaFilter(final String clientId, final String defaultTopic, final Producer<byte[], byte[]> producer) {
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
        try {
            producer.send(new KeyedMessage<byte[], byte[]>(defaultTopic, msg.asBytes()));
        } catch (FailedToSendMessageException e) {
            log.warn("Failed to send message", e);
            failedToSendMessageExceptions.incrementAndGet();
        }
    }

    @Override
    public JsonObject getStats() {
        return new JsonObject()
                .add("default_topic", defaultTopic)
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
