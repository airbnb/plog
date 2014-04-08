package com.airbnb.plog.kafka;

import com.airbnb.plog.filters.Filter;
import com.airbnb.plog.filters.FilterProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import io.netty.channel.ChannelHandler;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class KafkaProvider implements FilterProvider {
    private final static AtomicInteger clientId = new AtomicInteger();

    @Override
    public Filter getFilter(Config config) throws Exception {
        final String defaultTopic = config.getString("default_topic");

        final Properties properties = new Properties();
        for (Map.Entry<String, ConfigValue> kv : config.getConfig("producer_config").entrySet())
            properties.put(kv.getKey(), kv.getValue().unwrapped().toString());

        final String clientId = "plog_" +
                InetAddress.getLocalHost().getHostName() + "_" +
                KafkaProvider.clientId.getAndIncrement();

        properties.put("client.id", clientId);

        log.info("Using producer with properties {}", properties);

        final ProducerConfig producerConfig = new ProducerConfig(properties);
        final Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(producerConfig);

        return new KafkaFilter(producer, defaultTopic);
    }
}
