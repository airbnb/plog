package com.airbnb.plog.stress;

import com.airbnb.plog.client.fragmentation.Fragmenter;
import com.airbnb.plog.common.Murmur3;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("CallToSystemExit")
@Slf4j
public final class PlogStress {
    private final MetricRegistry registry = new MetricRegistry();

    public static void main(String[] args) {
        new PlogStress().run(ConfigFactory.load());
    }

    @SuppressWarnings("OverlyLongMethod")
    private void run(Config config) {
        System.err.println(
                "      _\n" +
                        " _ __| |___  __ _\n" +
                        "| '_ \\ / _ \\/ _` |\n" +
                        "| .__/_\\___/\\__, |\n" +
                        "|_|         |___/ stress"
        );

        final Config stressConfig = config.getConfig("plog.stress");

        final int threadCount = stressConfig.getInt("threads");
        log.info("Using {} threads", threadCount);

        final int rate = stressConfig.getInt("rate");
        final RateLimiter rateLimiter = RateLimiter.create(rate);

        final int socketRenewRate = stressConfig.getInt("renew_rate");
        final int minSize = stressConfig.getInt("min_size");
        final int maxSize = stressConfig.getInt("max_size");
        final int sizeIncrements = stressConfig.getInt("size_increments");
        final double sizeExponent = stressConfig.getDouble("size_exponent");

        final int sizeDelta = maxSize - minSize;
        final int differentSizes = sizeDelta / sizeIncrements;
        if (differentSizes == 0) {
            throw new RuntimeException("No sizes! Decrease plog.stress.size_increments");
        }

        final int stopAfter = stressConfig.getInt("stop_after");

        final int packetSize = stressConfig.getInt("udp.size");
        final int bufferSize = stressConfig.getInt("udp.SO_SNDBUF");

        final Fragmenter fragmenter = new Fragmenter(packetSize);

        final Random random = new Random(stressConfig.getLong("seed"));
        final byte[] randomBytes = new byte[maxSize];
        random.nextBytes(randomBytes);
        final ByteBuf randomMessage = Unpooled.wrappedBuffer(randomBytes);

        log.info("Generating {} different hashes", differentSizes);
        final int[] precomputedHashes = new int[differentSizes];
        for (int i = 0; i < differentSizes; i++) {
            precomputedHashes[i] = Murmur3.hash32(randomMessage, 0, minSize + sizeIncrements * i, 0);
        }

        final ByteBufAllocator allocator = new PooledByteBufAllocator();

        final double packetLoss = stressConfig.getDouble("udp.loss");

        final Meter socketMeter = registry.meter("Sockets used");
        final Meter messageMeter = registry.meter("Messages sent");
        final Meter packetMeter = registry.meter("Packets sent");
        final Meter sendFailureMeter = registry.meter("Send failures");
        final Meter lossMeter = registry.meter("Packets dropped");
        final Histogram messageSizeHistogram = registry.histogram("Message size");
        final Histogram packetSizeHistogram = registry.histogram("Packet size");

        final InetSocketAddress target = new InetSocketAddress(stressConfig.getString("host"), stressConfig.getInt("port"));

        log.info("Starting with config {}", config);

        final long consoleRate = stressConfig.getDuration("console.interval", TimeUnit.MILLISECONDS);
        ConsoleReporter.forRegistry(registry).build().start(consoleRate, TimeUnit.MILLISECONDS);

        for (int i = 0; i < threadCount; i++) {
            new Thread("stress_" + i) {
                private DatagramChannel channel = null;

                @Override
                public void run() {
                    try {
                        for (int sent = 0; sent < stopAfter; sent++, messageMeter.mark()) {
                            if (sent % socketRenewRate == 0) {
                                if (channel != null) {
                                    channel.close();
                                }
                                channel = DatagramChannel.open();
                                channel.socket().setSendBufferSize(bufferSize);
                                socketMeter.mark();
                            }

                            // global rate limiting
                            rateLimiter.acquire();

                            final int sizeIndex = (int) (Math.pow(random.nextDouble(), sizeExponent) * differentSizes);
                            final int messageSize = minSize + sizeIncrements * sizeIndex;
                            final int hash = precomputedHashes[sizeIndex];

                            messageSizeHistogram.update(messageSize);

                            final ByteBuf[] fragments = fragmenter.fragment(allocator, randomMessage, null, sent, messageSize, hash);

                            for (ByteBuf fragment : fragments) {
                                if (random.nextDouble() < packetLoss) {
                                    lossMeter.mark();
                                } else {
                                    final int packetSize = fragment.readableBytes();
                                    final ByteBuffer buffer = fragment.nioBuffer();

                                    try {
                                        channel.send(buffer, target);
                                        packetSizeHistogram.update(packetSize);
                                        packetMeter.mark();
                                    } catch (SocketException e) {
                                        sendFailureMeter.mark();
                                    }
                                }
                                fragment.release();
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        System.exit(1);
                    }
                }
            }.start();
        }
    }
}
