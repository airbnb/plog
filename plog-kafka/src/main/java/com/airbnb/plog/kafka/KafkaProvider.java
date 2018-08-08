package com.airbnb.plog.kafka;

import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.handlers.HandlerProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValue;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public final class KafkaProvider implements HandlerProvider {
    private final static AtomicInteger clientId = new AtomicInteger();

    static class EncryptionConfig {
        public String encryptionKey;
        public String encryptionAlgorithm;
        public String encryptionTransformation;
        public String encryptionProvider;
    }

    @Override
    public Handler getHandler(Config config) throws Exception {
        final String defaultTopic = config.getString("default_topic");
        boolean propagate = false;
        try {
            propagate = config.getBoolean("propagate");
        } catch (ConfigException.Missing ignored) {}

        if ("null".equals(defaultTopic)) {
            log.warn("default topic is \"null\"; messages will be discarded unless tagged with kt:");
        }

        final Properties properties = new Properties();
        for (Map.Entry<String, ConfigValue> kv : config.getConfig("producer_config").entrySet()) {
            properties.put(kv.getKey(), kv.getValue().unwrapped().toString());
        }

        final String clientId = "plog_" +
                InetAddress.getLocalHost().getHostName() + "_" +
                KafkaProvider.clientId.getAndIncrement();

        properties.put("client.id", clientId);
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");

        log.info("Using producer with properties {}", properties);

        final ProducerConfig producerConfig = new ProducerConfig(properties);
        final Producer<String, byte[]> producer = new Producer<String, byte[]>(producerConfig);

        EncryptionConfig encryptionConfig = new EncryptionConfig();
        try {
            Config encryption = config.getConfig("encryption");
            encryptionConfig.encryptionKey = encryption.getString("key");
            encryptionConfig.encryptionAlgorithm = encryption.getString("algorithm");
            encryptionConfig.encryptionTransformation = encryption.getString("transformation");
            encryptionConfig.encryptionProvider = encryption.getString("provider");
        } catch (ConfigException.Missing ignored) {
            encryptionConfig = null;
        }

        return new KafkaHandler(clientId, propagate, defaultTopic, producer, encryptionConfig);
    }
}
