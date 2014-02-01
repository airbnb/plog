package com.github.pcarrier.plog;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

public class App {

    private final static int MAX_LINE_LENGTH = Integer.parseInt(System.getProperty("plog.max_line_length", "1048576"));
    private final static int PORT = Integer.parseInt(System.getProperty("plog.port", "54321"));
    private final static Charset CHARSET = Charset.forName(System.getProperty("plog.charset", "UTF-8"));
    private final static String TOPIC = System.getProperty("plog.topic", "flog");
    private final static String METADATA_BROKER_LIST = "metadata.broker.list";
    private final static String SERIALIZER_CLASS = "serializer.class";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = System.getProperties();
        if (properties.getProperty(SERIALIZER_CLASS) == null)
            properties.setProperty(SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        if (properties.getProperty(METADATA_BROKER_LIST) == null)
            properties.setProperty(METADATA_BROKER_LIST, "127.0.0.1:9092");

        new App().run(properties);
    }

    ;

    private void run(Properties properties) {
        ProducerConfig cfg = new ProducerConfig(properties);

        final Producer<String, String> producer = new Producer<String, String>(cfg);
        final KafkaForwarder forwarder = new KafkaForwarder(TOPIC, producer);

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
                                .addLast(new LineBasedFrameDecoder(MAX_LINE_LENGTH))
                                .addLast(new StringDecoder(CHARSET))
                                .addLast(forwarder);
                    }
                }).bind(new InetSocketAddress(PORT)).addListener(futureListener);

        new Bootstrap().group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new MessageToMessageDecoder<DatagramPacket>() {
                                    @Override
                                    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
                                        out.add(msg.content().toString(CHARSET));
                                    }
                                })
                                .addLast(forwarder);
                    }
                }).bind(new InetSocketAddress(PORT)).addListener(futureListener);

        System.out.println("Started");
    }
}
