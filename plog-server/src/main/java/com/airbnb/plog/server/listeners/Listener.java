package com.airbnb.plog.server.listeners;

import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.handlers.HandlerProvider;
import com.airbnb.plog.server.pipeline.EndOfPipeline;
import com.airbnb.plog.server.stats.SimpleStatisticsReporter;
import com.airbnb.plog.server.stats.StatisticsReporter;
import com.google.common.util.concurrent.AbstractService;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;

@Slf4j
abstract class Listener extends AbstractService {
    @Getter
    private final Config config;
    @Getter
    private final StatisticsReporter stats;
    private final EndOfPipeline eopHandler;
    private EventLoopGroup eventLoopGroup = null;

    public Listener(Config config) {
        this(config, new SimpleStatisticsReporter());
    }

    public Listener(Config config, StatisticsReporter reporter) {
        this.config = config;
        this.stats = reporter;
        this.eopHandler = new EndOfPipeline(stats);
    }

    protected abstract StartReturn start();

    void finalizePipeline(ChannelPipeline pipeline)
            throws Exception {

        int i = 0;

        for (Config handlerConfig : config.getConfigList("handlers")) {
            final String providerName = handlerConfig.getString("provider");
            log.debug("Loading provider for {}", providerName);

            final Class<?> providerClass = Class.forName(providerName);
            final Constructor<?> providerConstructor = providerClass.getConstructor();
            final HandlerProvider provider = (HandlerProvider) providerConstructor.newInstance();
            final Handler handler = provider.getHandler(handlerConfig);

            pipeline.addLast(i + ':' + handler.getName(), handler);
            stats.appendHandler(handler);

            i++;
        }

        pipeline.addLast(eopHandler);
    }

    @Override
    protected void doStart() {
        final StartReturn startReturn = start();
        final ChannelFuture bindFuture = startReturn.getBindFuture();
        bindFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (bindFuture.isDone()) {
                    if (bindFuture.isSuccess()) {
                        log.info("{} bound successful", this);
                        notifyStarted();
                    } else if (bindFuture.isCancelled()) {
                        log.info("{} bind cancelled", this);
                        notifyFailed(new ChannelException("Cancelled"));
                    } else {
                        final Throwable cause = bindFuture.cause();
                        log.error("{} failed to bind", this, cause);
                        notifyFailed(cause);
                    }
                }
            }
        });
        this.eventLoopGroup = startReturn.getEventLoopGroup();
    }

    @Override
    protected void doStop() {
        //noinspection unchecked
        eventLoopGroup.shutdownGracefully().addListener(new GenericFutureListener() {
            @Override
            public void operationComplete(Future future) throws Exception {
                if (future.isSuccess()) {
                    notifyStopped();
                } else {
                    Throwable failure = new Exception("Netty event loop did not shutdown properly", future.cause());
                    log.error("Shutdown failed", failure);
                    notifyFailed(failure);
                }
            }
        });
    }

}
