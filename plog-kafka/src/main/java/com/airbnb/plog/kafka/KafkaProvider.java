package com.airbnb.plog.kafka;

import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.handlers.HandlerProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class KafkaProvider implements HandlerProvider {
    private final static AtomicInteger clientId = new AtomicInteger();

    @Override
    public Handler getHandler(Config config) throws Exception {
        final String defaultTopic = config.getString("default_topic");

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

        log.info("Using producer with properties {}", properties);

        final ProducerConfig producerConfig = new ProducerConfig(properties);
        final Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(producerConfig);

        return new KafkaHandler(clientId, defaultTopic, producer);
    }
}
