package com.airbnb.plog;

import com.airbnb.plog.commands.FourLetterCommandHandler;
import com.airbnb.plog.fragmentation.Defragmenter;
import com.airbnb.plog.kafka.KafkaForwarder;
import com.airbnb.plog.stats.SimpleStatisticsReporter;
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
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class App {
    private final static String METADATA_BROKER_LIST = "metadata.broker.list";
    private final static String CLIENT_ID = "client.id";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties systemProperties = System.getProperties();
        if (systemProperties.getProperty(METADATA_BROKER_LIST) == null)
            systemProperties.setProperty(METADATA_BROKER_LIST, "127.0.0.1:9092");
        if (systemProperties.getProperty(CLIENT_ID) == null)
            systemProperties.setProperty(CLIENT_ID, "plog_" + InetAddress.getLocalHost().getHostName());

        final Config config = ConfigFactory.load();
        new App().run(systemProperties, config);
    }

    private void run(final Properties properties, final Config config) {
        final SimpleStatisticsReporter stats = new SimpleStatisticsReporter(properties.getProperty(CLIENT_ID));
        final KafkaForwarder forwarder = new KafkaForwarder(
                config.getString("plog.topic"),
                new Producer<byte[], byte[]>(new ProducerConfig(properties)),
                stats);
        final ExecutorService threadPool = Executors.newFixedThreadPool(config.getInt("plog.threads"));
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

        if (config.getBoolean("plog.udp.enabled")) {
            startUDP(config, stats, forwarder, threadPool, group)
                    .addListener(futureListener);
        }

        if (config.getBoolean("plog.tcp.enabled"))
            startTCP(config, forwarder, group)
                    .addListener(futureListener);

        log.info("Started");
    }

    private ChannelFuture startTCP(final Config config,
                                   final KafkaForwarder forwarder,
                                   final EventLoopGroup group) {
        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new LineBasedFrameDecoder(config.getInt("plog.tcp.max_line")))
                                .addLast(new Message.ByteBufToMessageDecoder())
                                .addLast(forwarder);
                    }
                }).bind(new InetSocketAddress(config.getInt("plog.tcp.port")));
    }

    private ChannelFuture startUDP(final Config config,
                                   final SimpleStatisticsReporter stats,
                                   final KafkaForwarder forwarder,
                                   final ExecutorService threadPool,
                                   final EventLoopGroup group) {
        final ProtocolDecoder protocolDecoder = new ProtocolDecoder(stats);
        final Defragmenter defragmenter = new Defragmenter(stats, config.getConfig("plog.defrag"));
        stats.withDefrag(defragmenter);
        final FourLetterCommandHandler commandHandler = new FourLetterCommandHandler(stats, config);

        return new Bootstrap().group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF,
                        config.getInt("plog.udp.SO_RCVBUF"))
                .option(ChannelOption.SO_SNDBUF,
                        config.getInt("plog.udp.SO_SNDBUF"))
                .option(ChannelOption.RCVBUF_ALLOCATOR,
                        new FixedRecvByteBufAllocator(config.getInt("plog.udp.RECV_SIZE")))
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new SimpleChannelInboundHandler<DatagramPacket>(false) {
                                    @Override
                                    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) throws Exception {
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
                                .addLast(forwarder)
                                .addLast(new SimpleChannelInboundHandler<Void>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, Void msg) throws Exception {
                                        log.error("Some seriously weird stuff going on here!");
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                        log.error("Exception down the UDP pipeline", cause);
                                        stats.exception();
                                    }
                                });
                    }
                })
                .bind(new InetSocketAddress(config.getInt("plog.udp.port")));
    }
}
