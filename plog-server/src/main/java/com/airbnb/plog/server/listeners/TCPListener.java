package com.airbnb.plog.server.listeners;

import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

import java.net.InetSocketAddress;

public class TCPListener extends Listener {
    public TCPListener(Config config) {
        super(config);
    }

    @Override
    public ChannelFuture start() {
        final Config config = getConfig();

        return new ServerBootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        final ChannelPipeline pipeline = channel.pipeline();
                        pipeline
                                .addLast(new LineBasedFrameDecoder(config.getInt("max_line")))
                                .addLast(new ByteBufToMessageDecoder());
                        finalizePipeline(pipeline);
                    }
                }).bind(new InetSocketAddress(config.getString("host"), config.getInt("port")));
    }
}
