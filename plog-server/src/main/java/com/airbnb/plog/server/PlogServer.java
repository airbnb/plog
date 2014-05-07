package com.airbnb.plog.server;

import com.airbnb.plog.server.listeners.TCPListener;
import com.airbnb.plog.server.listeners.UDPListener;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PlogServer {
    public static void main(String[] args) {
        log.info("Starting...");

        System.err.println(
                "      _\n" +
                        " _ __| |___  __ _\n" +
                        "| '_ \\ / _ \\/ _` |\n" +
                        "| .__/_\\___/\\__, |\n" +
                        "|_|         |___/"
        );
        new PlogServer().run(ConfigFactory.load());
    }

    private void run(Config config) {
        final ChannelFutureListener futureListener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isDone() && !channelFuture.isSuccess()) {
                    log.error("Channel failure", channelFuture.cause());
                    System.exit(1);
                }
            }
        };

        final Config plogServer = config.getConfig("plog.server");

        final Config globalDefaults = plogServer.getConfig("defaults");

        final Config udpConfig = plogServer.getConfig("udp");
        final Config udpDefaults = udpConfig.getConfig("defaults").withFallback(globalDefaults);

        final Config tcpConfig = plogServer.getConfig("tcp");
        final Config tcpDefaults = tcpConfig.getConfig("defaults").withFallback(globalDefaults);

        for (final Config cfg : udpConfig.getConfigList("listeners"))
            new UDPListener(cfg.withFallback(udpDefaults)).start().addListener(futureListener);

        for (final Config cfg : tcpConfig.getConfigList("listeners"))
            new TCPListener(cfg.withFallback(tcpDefaults)).start().addListener(futureListener);

        log.info("Started with config {}", config);
    }
}
