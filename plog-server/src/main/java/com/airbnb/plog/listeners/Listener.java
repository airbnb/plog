package com.airbnb.plog.listeners;

import com.airbnb.plog.EndOfPipeline;
import com.airbnb.plog.filters.FilterProvider;
import com.airbnb.plog.stats.SimpleStatisticsReporter;
import com.typesafe.config.Config;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.net.UnknownHostException;

@Slf4j
public abstract class Listener {
    @Getter
    private final Config config;

    @Getter
    private final SimpleStatisticsReporter stats;

    private final EndOfPipeline eopHandler;

    public Listener(int id, Config config)
            throws UnknownHostException {
        this.config = config;

        this.stats = new SimpleStatisticsReporter();
        this.eopHandler = new EndOfPipeline(stats);
    }

    public abstract ChannelFuture start(final EventLoopGroup group);

    void finalizePipeline(ChannelPipeline pipeline)
            throws Exception {

        for (Config filterConfig : config.getConfigList("filters")) {
            final String providerName = filterConfig.getString("provider");
            log.debug("Loading provider for {}", providerName);

            final Class<?> providerClass = Class.forName(providerName);
            final Constructor<?> providerConstructor = providerClass.getConstructor();
            final FilterProvider provider = (FilterProvider) providerConstructor.newInstance();

            pipeline.addLast(provider.getFilter(filterConfig));
        }

        pipeline.addLast(eopHandler);
    }
}
