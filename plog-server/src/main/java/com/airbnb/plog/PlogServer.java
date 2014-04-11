package com.airbnb.plog;

import com.airbnb.plog.listeners.TCPListener;
import com.airbnb.plog.listeners.UDPListener;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.UnknownHostException;

@Slf4j
public class PlogServer {
    public static void main(String[] args)
            throws UnknownHostException {
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

    private void run(Config config)
            throws UnknownHostException {
        final EventLoopGroup group = new NioEventLoopGroup();

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

        for (final Config cfg : udpConfig.getConfigList("listeners"))
            new UDPListener(cfg.withFallback(udpDefaults)).start(group).addListener(futureListener);

        for (final Config cfg : tcpConfig.getConfigList("listeners"))
            new TCPListener(cfg.withFallback(tcpDefaults)).start(group).addListener(futureListener);

        log.info("Started with config {}", config);
    }
}
