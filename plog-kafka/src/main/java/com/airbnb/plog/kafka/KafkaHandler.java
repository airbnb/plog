package com.airbnb.plog.kafka;

import com.airbnb.plog.Message;
import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.kafka.KafkaProvider.EncryptionConfig;
import com.eclipsesource.json.JsonObject;
import com.yammer.metrics.core.Meter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
@Slf4j
public final class KafkaHandler extends SimpleChannelInboundHandler<Message> implements Handler {
    private final String defaultTopic;
    private final boolean propagate;
    private final Producer<Integer, byte[]> producer;
    private final AtomicLong failedToSendMessageExceptions = new AtomicLong(), seenMessages = new AtomicLong();
    private final ProducerStats producerStats;
    private final ProducerTopicMetrics producerAllTopicsStats;
    private final EncryptionConfig encryptionConfig;
    private SecretKeySpec keySpec = null;
    private final Random random = new Random();
    private Integer messageKey = random.nextInt();

    protected KafkaHandler(
            final String clientId,
            final boolean propagate,
            final String defaultTopic,
            final Producer<Integer, byte[]> producer,
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

        boolean sawKtTag = false;

        for (String tag : msg.getTags()) {
            if (tag.startsWith("kt:")) {
                sawKtTag = true;
                sendOrReportFailure(tag.substring(3), payload);
            }
        }

        if (!sawKtTag) {
            sendOrReportFailure(defaultTopic, payload);
        }

        if (propagate) {
            msg.retain();
            ctx.fireChannelRead(msg);
        }
    }

    private boolean sendOrReportFailure(String topic, final byte[] msg) {
        final boolean nonNullTopic = !("null".equals(topic));
        if (nonNullTopic) {
            try {
                final Integer key = getMessageKey();
                producer.send(new KeyedMessage<Integer, byte[]>(topic, key, msg));
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

  /**
   * In order to better distribute the messages over the topic partitions,
   * without limiting the batching efficiency, the same key is used every 1000 messages.
   *
   * The key is randomly generated every 1000 messages.
   *
   * @return
   */
  private Integer getMessageKey() {
      if (seenMessages.longValue() % 1000 == 0) {
          messageKey = random.nextInt();
      }
      return messageKey;
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
