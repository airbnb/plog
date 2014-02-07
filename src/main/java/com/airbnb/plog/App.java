package com.airbnb.plog;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Properties;

public class App {
    private final static String METADATA_BROKER_LIST = "metadata.broker.list";
    private final static String SERIALIZER_CLASS = "serializer.class";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties kafkaProperties = System.getProperties();
        if (kafkaProperties.getProperty(SERIALIZER_CLASS) == null)
            kafkaProperties.setProperty(SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        if (kafkaProperties.getProperty(METADATA_BROKER_LIST) == null)
            kafkaProperties.setProperty(METADATA_BROKER_LIST, "127.0.0.1:9092");

        final Config config = ConfigFactory.load();
        new App().run(kafkaProperties, config);
    }

    private void run(Properties properties, Config config) {
        final Config plogConfig = config.getConfig("plog");
        final Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(properties));
        final KafkaForwarder forwarder = new KafkaForwarder(plogConfig.getString("topic"), producer);
        final Charset charset = Charset.forName(plogConfig.getString("charset"));
        final int maxLineLength = plogConfig.getInt("max_line_length");
        final int port = plogConfig.getInt("port");
        final Statistics stats = new Statistics();
        final PlogPDecoder plogPDecoder = new PlogPDecoder(charset, stats);
        final PlogDefragmenter plogDefragmenter = new PlogDefragmenter(stats, plogConfig.getConfig("defrag"));
        final PlogCommandHandler commandHandler = new PlogCommandHandler(stats, config);

        final EventLoopGroup group = new NioEventLoopGroup();

        final ChannelFutureListener futureListener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isDone() && !channelFuture.isSuccess()) {
                    System.err.println(channelFuture.cause());
                    System.exit(1);
                }
            }
        };

        new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_LINGER, 0)
                .handler(new LoggingHandler(LogLevel.WARN))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new LineBasedFrameDecoder(maxLineLength))
                                .addLast(new StringDecoder(charset))
                                .addLast(forwarder);
                    }
                }).bind(new InetSocketAddress(port)).addListener(futureListener);

        new Bootstrap().group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(plogPDecoder)
                                .addLast(plogDefragmenter)
                                .addLast(commandHandler)
                                .addLast(forwarder);
                    }
                }).bind(new InetSocketAddress(port)).addListener(futureListener);

        System.out.println("Started");
    }
}
