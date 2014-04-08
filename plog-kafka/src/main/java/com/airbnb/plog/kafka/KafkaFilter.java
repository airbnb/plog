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
        final JsonObject result = new JsonObject();
        result.add("count", meter.count());
        final JsonArray rates = new JsonArray();
        result.add("rate", rates);
        rates.add(meter.oneMinuteRate());
        rates.add(meter.fiveMinuteRate());
        rates.add(meter.fifteenMinuteRate());
        return result;
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
        final JsonObject stats = new JsonObject();

        stats.add("failed_to_send", failedToSendMessageExceptions.get());

        stats.add("failed_send_rate", meterToJsonObject(producerStats.failedSendRate()));
        stats.add("resend_rate", meterToJsonObject(producerStats.resendRate()));
        stats.add("serialization_error_rate", meterToJsonObject(producerStats.serializationErrorRate()));

        stats.add("message_rate", meterToJsonObject(producerAllTopicsStats.messageRate()));
        stats.add("dropped_message_rate", meterToJsonObject(producerAllTopicsStats.droppedMessageRate()));
        stats.add("byte_rate", meterToJsonObject(producerAllTopicsStats.byteRate()));

        return stats;
    }
}
