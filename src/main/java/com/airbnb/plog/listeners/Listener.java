package com.airbnb.plog.listeners;

import com.airbnb.plog.EndOfPipeline;
import com.airbnb.plog.filters.FilterProvider;
import com.airbnb.plog.sinks.ConsoleSink;
import com.airbnb.plog.sinks.KafkaSink;
import com.airbnb.plog.sinks.Sink;
import com.airbnb.plog.stats.SimpleStatisticsReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

@Slf4j
public abstract class Listener {
    @Getter
    private final Config config;

    @Getter
    private final Sink sink;

    @Getter
    private final SimpleStatisticsReporter stats;

    @Getter
    private final EndOfPipeline eopHandler;

    public Listener(int id, Config config)
            throws UnknownHostException {
        this.config = config;

        final String clientId = "plog_" + InetAddress.getLocalHost().getHostName() + "_" + id;

        this.stats = new SimpleStatisticsReporter(clientId);

        this.eopHandler = new EndOfPipeline(stats);

        final String topic = config.getString("topic");
        if ("STDOUT".equals(topic)) {
            log.info("Using STDOUT");
            this.sink = new ConsoleSink();
        } else {
            final Properties kafkaProperties = new Properties();
            for (Map.Entry<String, ConfigValue> kv : config.getConfig("kafka").entrySet())
                kafkaProperties.put(kv.getKey(), kv.getValue().unwrapped().toString());
            kafkaProperties.put("client.id", clientId);

            log.info("Using Kafka with properties {}", kafkaProperties);

            final Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(new ProducerConfig(kafkaProperties));
            this.sink = new KafkaSink(topic, producer, stats);
        }
    }

    public abstract ChannelFuture start(final EventLoopGroup group);

    void appendFilters(ChannelPipeline pipeline)
            throws Exception {
        for (Config filterConfig : config.getConfigList("filters")) {
            final String providerName = filterConfig.getString("provider");
            log.debug("Loading provider for {}", providerName);

            final Class<?> providerClass = Class.forName(providerName);
            final Constructor<?> providerConstructor = providerClass.getConstructor();
            final FilterProvider provider = (FilterProvider) providerConstructor.newInstance();

            pipeline.addLast(provider.getFilter(filterConfig));
        }
    }
}
