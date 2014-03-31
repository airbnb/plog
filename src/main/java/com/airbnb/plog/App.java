package com.airbnb.plog;

import com.airbnb.plog.commands.FourLetterCommandHandler;
import com.airbnb.plog.fragmentation.Defragmenter;
import com.airbnb.plog.sinks.ConsoleSink;
import com.airbnb.plog.sinks.KafkaSink;
import com.airbnb.plog.sinks.Sink;
import com.airbnb.plog.stats.SimpleStatisticsReporter;
import com.airbnb.plog.stats.StatisticsReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class App {
    private final static String METADATA_BROKER_LIST = "metadata.broker.list";
    private final static String CLIENT_ID = "client.id";

    private final Properties kafkaProperties;
    private final Config config;

    // Lazily initialized
    private Producer<byte[], byte[]> producer = null;

    public App(Properties systemProperties, Config config) {
        this.kafkaProperties = systemProperties;
        this.config = config;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties systemProperties = System.getProperties();
        if (systemProperties.getProperty(METADATA_BROKER_LIST) == null)
            systemProperties.setProperty(METADATA_BROKER_LIST, "127.0.0.1:9092");
        if (systemProperties.getProperty(CLIENT_ID) == null)
            systemProperties.setProperty(CLIENT_ID, "plog_" + InetAddress.getLocalHost().getHostName());

        new App(systemProperties, ConfigFactory.load()).run();
    }

    private SimpleStatisticsReporter makeStats() {
        return new SimpleStatisticsReporter(kafkaProperties.getProperty(CLIENT_ID));
    }

    private synchronized Producer<byte[], byte[]> getProducer() {
        if (producer == null)
            producer = new Producer<byte[], byte[]>(new ProducerConfig(kafkaProperties));
        return producer;
    }

    private Sink getSink(Config config, StatisticsReporter stats) {
        final String topic = config.getString("topic");
        if ("STDOUT".equals(topic))
            return new ConsoleSink();
        else
            return new KafkaSink(topic, getProducer(), stats);
    }

    private void run() {
        final ExecutorService threadPool =
                Executors.newFixedThreadPool(config.getInt("plog.threads"));

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

        final List<? extends Config> udpConfigs = config.getConfigList("plog.udp");
        if (!udpConfigs.isEmpty()) {
            final SimpleStatisticsReporter stats = makeStats();
            final FourLetterCommandHandler flch = new FourLetterCommandHandler(stats, config);
            for (Config udpConfig : udpConfigs)
                startUDP(udpConfig, flch, threadPool, group)
                        .addListener(futureListener);
        }


        for (Config tcpConfig : config.getConfigList("plog.tcp")) {
            startTCP(tcpConfig, group)
                    .addListener(futureListener);
        }

        log.info("Started");
    }

    private ChannelFuture startTCP(final Config tcpConfig,
                                   final EventLoopGroup group) {
        final SimpleStatisticsReporter stats = makeStats();
        final EndOfPipeline eopHandler = new EndOfPipeline(stats);
        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new LineBasedFrameDecoder(tcpConfig.getInt("max_line")))
                                .addLast(new Message.ByteBufToMessageDecoder())
                                .addLast(getSink(tcpConfig, stats))
                                .addLast(eopHandler);
                    }
                }).bind(new InetSocketAddress(tcpConfig.getString("host"), tcpConfig.getInt("port")));
    }

    private ChannelFuture startUDP(final Config udpConfig,
                                   final FourLetterCommandHandler commandHandler,
                                   final ExecutorService threadPool,
                                   final EventLoopGroup group) {
        final SimpleStatisticsReporter stats = makeStats();
        final EndOfPipeline eopHandler = new EndOfPipeline(stats);
        final ProtocolDecoder protocolDecoder = new ProtocolDecoder(stats);

        final Defragmenter defragmenter = new Defragmenter(stats, udpConfig.getConfig("defrag"));
        stats.withDefrag(defragmenter);

        return new Bootstrap().group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF,
                        udpConfig.getInt("SO_RCVBUF"))
                .option(ChannelOption.SO_SNDBUF,
                        udpConfig.getInt("SO_SNDBUF"))
                .option(ChannelOption.RCVBUF_ALLOCATOR,
                        new FixedRecvByteBufAllocator(udpConfig.getInt("RECV_SIZE")))
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new SimpleChannelInboundHandler<DatagramPacket>(false) {
                                    @Override
                                    protected void channelRead0(final ChannelHandlerContext ctx,
                                                                final DatagramPacket msg)
                                            throws Exception {
                                        threadPool.submit(new Runnable() {
                                            @Override
                                            public void run() {
                                                ctx.fireChannelRead(msg);
                                            }
                                        });
                                    }
                                })
                                .addLast(protocolDecoder)
                                .addLast(defragmenter)
                                .addLast(commandHandler)
                                .addLast(getSink(udpConfig, stats))
                                .addLast(eopHandler);
                    }
                })
                .bind(new InetSocketAddress(udpConfig.getString("host"), udpConfig.getInt("port")));
    }
}
