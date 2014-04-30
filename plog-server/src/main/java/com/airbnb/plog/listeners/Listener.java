package com.airbnb.plog.listeners;

import com.airbnb.plog.EndOfPipeline;
import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.handlers.HandlerProvider;
import com.airbnb.plog.stats.SimpleStatisticsReporter;
import com.typesafe.config.Config;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
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

    public Listener(Config config)
            throws UnknownHostException {
        this.config = config;

        this.stats = new SimpleStatisticsReporter();
        this.eopHandler = new EndOfPipeline(stats);
    }

    public abstract ChannelFuture start();

    void finalizePipeline(ChannelPipeline pipeline)
            throws Exception {

        for (Config handlerConfig : config.getConfigList("handlers")) {
            final String providerName = handlerConfig.getString("provider");
            log.debug("Loading provider for {}", providerName);

            final Class<?> providerClass = Class.forName(providerName);
            final Constructor<?> providerConstructor = providerClass.getConstructor();
            final HandlerProvider provider = (HandlerProvider) providerConstructor.newInstance();
            final Handler handler = provider.getHandler(handlerConfig);

            pipeline.addLast(handler.getName(), handler);
            stats.appendHandler(handler);
        }

        pipeline.addLast(eopHandler);
    }
}
