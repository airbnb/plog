package com.airbnb.plog.listeners;

import com.airbnb.plog.Message;
import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;


public class HTTPListener extends Listener {
    public HTTPListener(int id, Config config) throws UnknownHostException {
        super(id, config);
    }

    @Override
    public ChannelFuture start(EventLoopGroup group) {
        final Config config = getConfig();

        return new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        final ChannelPipeline pipeline = channel.pipeline();
                        pipeline
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(config.getInt("max_content_length")))
                                .addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
                                        if (msg.getMethod() == HttpMethod.POST &&
                                                msg.getUri().equals("/push")) {
                                            final ByteBuf content = msg.content();
                                            content.retain();
                                            ctx.fireChannelRead(content);
                                        }
                                    }
                                })
                        .addLast(new Message.ByteBufToMessageDecoder());
                        appendFilters(pipeline);
                        pipeline
                                .addLast(getSink())
                                .addLast(getEopHandler());
                    }
                }).bind(new InetSocketAddress(config.getString("host"), config.getInt("port")));

    }
}
