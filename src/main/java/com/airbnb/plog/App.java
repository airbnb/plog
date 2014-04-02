package com.airbnb.plog;

import com.airbnb.plog.listeners.TCPListener;
import com.airbnb.plog.listeners.UDPListener;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.UnknownHostException;
import java.util.List;

@Slf4j
public class App {
    public static void main(String[] args)
            throws UnknownHostException {
        new App().run(ConfigFactory.load());
    }

    private void run(Config config)
            throws UnknownHostException {
        final ChannelFutureListener futureListener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isDone() && !channelFuture.isSuccess()) {
                    log.error("Channel failure", channelFuture.cause());
                    System.exit(1);
                }
            }
        };

        final Config plogConfig = config.getConfig("plog");

        final Config globalDefaults = plogConfig.getConfig("defaults");

        final Config udpConfig = plogConfig.getConfig("udp");
        final Config udpDefaults = udpConfig.getConfig("defaults").withFallback(globalDefaults);

        final Config tcpConfig = plogConfig.getConfig("tcp");
        final Config tcpDefaults = tcpConfig.getConfig("defaults").withFallback(globalDefaults);

        int listenerId = 0;


        final List<? extends Config> udpListeners = udpConfig.getConfigList("listeners");
        if (!udpListeners.isEmpty()) {
            final EventLoopGroup udpGroup = new OioEventLoopGroup();
            for (final Config cfg : udpListeners) {
                new UDPListener(listenerId, cfg.withFallback(udpDefaults)).start(udpGroup).addListener(futureListener);
                listenerId++;
            }
        }


        final List<? extends Config> tcpListeners = tcpConfig.getConfigList("listeners");
        if (!tcpListeners.isEmpty()) {
            final EventLoopGroup tcpGroup = new NioEventLoopGroup();
            for (final Config cfg : tcpListeners) {
                new TCPListener(listenerId, cfg.withFallback(tcpDefaults)).start(tcpGroup).addListener(futureListener);
                listenerId++;
            }
        }

        log.info("Started with config {}", config);
    }
}
