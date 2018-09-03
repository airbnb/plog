package com.airbnb.plog.kafka;

import com.airbnb.plog.kafka.KafkaProvider.EncryptionConfig;
import com.airbnb.plog.Message;
import com.airbnb.plog.handlers.Handler;
import com.eclipsesource.json.JsonObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.SerializationException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

@RequiredArgsConstructor
@Slf4j
public final class KafkaHandler extends SimpleChannelInboundHandler<Message> implements Handler {
    private final String defaultTopic;
    private final boolean propagate;
    private final KafkaProducer<String, byte[]> producer;
    private final AtomicLong failedToSendMessageExceptions = new AtomicLong();
    private final AtomicLong seenMessages = new AtomicLong();
    private final AtomicLong serializationErrors = new AtomicLong();

    private final EncryptionConfig encryptionConfig;
    private SecretKeySpec keySpec = null;

    private static final ImmutableMap<String, MetricName> SHORTNAME_TO_METRICNAME =
        ImmutableMap.<String, MetricName>builder()
            // Compatibility with Plog 4.0
            .put("message", new MetricName("record-send-rate", "producer-metrics"))
            .put("resend", new MetricName("record-retry-rate", "producer-metrics"))
            .put("failed_send", new MetricName("record-error-rate", "producer-metrics"))
            .put("dropped_message", new MetricName("record-error-rate", "producer-metrics"))
            .put("byte", new MetricName("outgoing-byte-rate", "producer-metrics"))
            .build();

    protected KafkaHandler(
            final String clientId,
            final boolean propagate,
            final String defaultTopic,
            final KafkaProducer<String, byte[]> producer,
            final EncryptionConfig encryptionConfig) {

        super();
        this.propagate = propagate;
        this.defaultTopic = defaultTopic;
        this.producer = producer;
        this.encryptionConfig = encryptionConfig;

        if (encryptionConfig != null) {
            final byte[] keyBytes = encryptionConfig.encryptionKey.getBytes();
            keySpec = new SecretKeySpec(keyBytes, encryptionConfig.encryptionAlgorithm);
            log.info("KafkaHandler start with encryption algorithm '"
                + encryptionConfig.encryptionAlgorithm + "' transformation '"
                + encryptionConfig.encryptionTransformation + "' provider '"
                + encryptionConfig.encryptionProvider + "'.");
        } else {
            log.info("KafkaHandler start without encryption.");
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        seenMessages.incrementAndGet();
        byte[] payload = msg.asBytes();
        if (encryptionConfig != null) {
            try {
                payload = encrypt(payload);
            } catch (Exception e) {
                log.error("Fail to encrypt message: ", e.getMessage());
            }
        }
        String kafkaTopic = defaultTopic;
        // Producer will simply do round-robin when a null partitionKey is provided
        String partitionKey = null;

        for (String tag : msg.getTags()) {
            if (tag.startsWith("kt:")) {
                kafkaTopic = tag.substring(3);
            } else if (tag.startsWith("pk:")) {
                partitionKey = tag.substring(3);
            }
        }

        sendOrReportFailure(kafkaTopic, partitionKey, payload);

        if (propagate) {
            msg.retain();
            ctx.fireChannelRead(msg);
        }
    }

    private boolean sendOrReportFailure(String topic, final String key, final byte[] msg) {
        final boolean nonNullTopic = !("null".equals(topic));
        if (nonNullTopic) {
            try {
                producer.send(new ProducerRecord<String, byte[]>(topic, key, msg));
            } catch (SerializationException e) {
                failedToSendMessageExceptions.incrementAndGet();
                serializationErrors.incrementAndGet();
            } catch (KafkaException e) {
                log.warn("Failed to send to topic {}", topic, e);
                failedToSendMessageExceptions.incrementAndGet();
            }
        }
        return nonNullTopic;
    }

    private byte[] encrypt(final byte[] plaintext) throws Exception {
        Cipher cipher = Cipher.getInstance(
            encryptionConfig.encryptionTransformation,encryptionConfig.encryptionProvider);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        // IV size is the same as a block size and cipher dependent.
        // This can be derived from consumer side by calling `cipher.getBlockSize()`.
        outputStream.write(cipher.getIV());
        outputStream.write(cipher.doFinal(plaintext));
        return outputStream.toByteArray();
    }

    @Override
    public JsonObject getStats() {

        Map<MetricName, ? extends Metric> metrics = producer.metrics();

        JsonObject stats = new JsonObject()
            .add("seen_messages", seenMessages.get())
            .add("failed_to_send", failedToSendMessageExceptions.get());

        // Map to Plog v4-style naming
        for (Map.Entry<String, MetricName> entry: SHORTNAME_TO_METRICNAME.entrySet()) {
            Metric metric = metrics.get(entry.getValue());
            if (metric != null) {
                stats.add(entry.getKey(), metric.value());
            } else {
                stats.add(entry.getKey(), 0.0);
            }
        }

        // Use default kafka naming, include all producer metrics
        for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
            double value = metric.getValue().value();
            if (value > -Double.MAX_VALUE && value < Double.MAX_VALUE) {
                stats.add(metric.getKey().name(), value);
            } else {
                stats.add(metric.getKey().name(), 0.0);
            }
        }

        return stats;
    }

    @Override
    public final String getName() {
        return "kafka";
    }
}
