package com.airbnb.plog.kafka;

import com.airbnb.plog.kafka.KafkaProvider.EncryptionConfig;
import com.airbnb.plog.Message;
import com.airbnb.plog.handlers.Handler;
import com.eclipsesource.json.JsonObject;
import com.yammer.metrics.core.Meter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Base64;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

@RequiredArgsConstructor
@Slf4j
public final class KafkaHandler extends SimpleChannelInboundHandler<Message> implements Handler {
    private final String defaultTopic;
    private final boolean propagate;
    private final Producer<byte[], byte[]> producer;
    private final AtomicLong failedToSendMessageExceptions = new AtomicLong(), seenMessages = new AtomicLong();
    private final ProducerStats producerStats;
    private final ProducerTopicMetrics producerAllTopicsStats;
    private final EncryptionConfig encryptionConfig;
    private SecretKeySpec keySpec = null;

    protected KafkaHandler(
            final String clientId,
            final boolean propagate,
            final String defaultTopic,
            final Producer<byte[], byte[]> producer,
            final EncryptionConfig encryptionConfig) {

        super();
        this.propagate = propagate;
        this.producerStats = ProducerStatsRegistry.getProducerStats(clientId);
        this.producerAllTopicsStats =
            ProducerTopicStatsRegistry.getProducerTopicStats(clientId).getProducerAllTopicsStats();
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

    private static JsonObject meterToJsonObject(Meter meter) {
        return new JsonObject()
                .add("count", meter.count())
                .add("rate", new JsonObject()
                        .add("1", meter.oneMinuteRate())
                        .add("5", meter.fiveMinuteRate())
                        .add("15", meter.fifteenMinuteRate()));
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
        byte[] partitionKey = null;

        for (String tag : msg.getTags()) {
            if (tag.startsWith("kt:")) {
                kafkaTopic = tag.substring(3);
            } else if (tag.startsWith("pk:")) {
              // Base64 decode the partitionKey to get the raw bytes
                partitionKey = Base64.getUrlDecoder().decode( tag.substring(3));
            }
        }

        sendOrReportFailure(kafkaTopic, partitionKey, payload);

        if (propagate) {
            msg.retain();
            ctx.fireChannelRead(msg);
        }
    }

    private boolean sendOrReportFailure(String topic, final byte[] key, final byte[] msg) {
        final boolean nonNullTopic = !("null".equals(topic));
        if (nonNullTopic) {
            try {
                producer.send(new KeyedMessage<byte[], byte[]>(topic, key, msg));
            } catch (FailedToSendMessageException e) {
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
        return new JsonObject()
                .add("default_topic", defaultTopic)
                .add("seen_messages", seenMessages.get())
                .add("failed_to_send", failedToSendMessageExceptions.get())
                .add("failed_send", meterToJsonObject(producerStats.failedSendRate()))
                .add("resend", meterToJsonObject(producerStats.resendRate()))
                .add("serialization_error", meterToJsonObject(producerStats.serializationErrorRate()))
                .add("message", meterToJsonObject(producerAllTopicsStats.messageRate()))
                .add("dropped_message", meterToJsonObject(producerAllTopicsStats.droppedMessageRate()))
                .add("byte", meterToJsonObject(producerAllTopicsStats.byteRate()));
    }

    @Override
    public final String getName() {
        return "kafka";
    }
}
