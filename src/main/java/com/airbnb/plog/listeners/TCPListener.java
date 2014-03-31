package com.airbnb.plog.listeners;

import com.airbnb.plog.Message;
import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class TCPListener extends Listener {
    public TCPListener(int id, Config config)
            throws UnknownHostException {
        super(id, config);
    }

    @Override
    public ChannelFuture start(final EventLoopGroup group) {
        final Config config = getConfig();

        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new LineBasedFrameDecoder(config.getInt("max_line")))
                                .addLast(new Message.ByteBufToMessageDecoder())
                                .addLast(getSink())
                                .addLast(getEopHandler());
                    }
                }).bind(new InetSocketAddress(config.getString("host"), config.getInt("port")));
    }
}
