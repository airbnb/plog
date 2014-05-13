package com.airbnb.plog.server;

import com.airbnb.plog.server.listeners.TCPListener;
import com.airbnb.plog.server.listeners.UDPListener;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("CallToSystemExit")
@Slf4j
public final class PlogServer {
    public static void main(String[] args) {
        log.info("Starting...");

        System.err.println(
                "      _\n" +
                        " _ __| |___  __ _\n" +
                        "| '_ \\ / _ \\/ _` |\n" +
                        "| .__/_\\___/\\__, |\n" +
                        "|_|         |___/ server"
        );
        new PlogServer().run(ConfigFactory.load());
    }

    private void run(Config config) {
        log.info("Starting with config {}", config);

        final Config plogServer = config.getConfig("plog.server");

        final Config globalDefaults = plogServer.getConfig("defaults");

        final Config udpConfig = plogServer.getConfig("udp");
        final Config udpDefaults = udpConfig.getConfig("defaults").withFallback(globalDefaults);

        final Config tcpConfig = plogServer.getConfig("tcp");
        final Config tcpDefaults = tcpConfig.getConfig("defaults").withFallback(globalDefaults);

        final ArrayList<Service> services = Lists.newArrayList();

        for (final Config cfg : udpConfig.getConfigList("listeners")) {
            services.add(new UDPListener(cfg.withFallback(udpDefaults)));
        }

        for (final Config cfg : tcpConfig.getConfigList("listeners")) {
            services.add(new TCPListener(cfg.withFallback(tcpDefaults)));
        }

        final long shutdownTime = plogServer.getDuration("shutdown_time", TimeUnit.MILLISECONDS);

        final ServiceManager manager = new ServiceManager(services);
        manager.addListener(new ServiceManager.Listener() {
            @Override
            public void healthy() {
                log.info("All listeners started!");
            }

            @Override
            public void failure(Service service) {
                log.error("Failure for listener {}", service);
                System.exit(1);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down...");
                try {
                    manager.stopAsync().awaitStopped(shutdownTime, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    log.warn("Did not shut down gracefully after {}ms!", shutdownTime, e);
                    System.exit(2);
                }
            }
        });

        manager.startAsync();
    }
}
