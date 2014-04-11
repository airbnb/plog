package com.airbnb.plog.listeners;

import com.airbnb.plog.ProtocolDecoder;
import com.airbnb.plog.commands.FourLetterCommandHandler;
import com.airbnb.plog.fragmentation.Defragmenter;
import com.airbnb.plog.stats.SimpleStatisticsReporter;
import com.typesafe.config.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPListener extends Listener {
    public UDPListener(Config config)
            throws UnknownHostException {
        super(config);
    }

    @Override
    public ChannelFuture start(EventLoopGroup group) {
        final Config config = getConfig();

        final SimpleStatisticsReporter stats = getStats();

        final ProtocolDecoder protocolDecoder = new ProtocolDecoder(stats);

        final Defragmenter defragmenter = new Defragmenter(stats, config.getConfig("defrag"));
        stats.withDefrag(defragmenter);

        final FourLetterCommandHandler flch = new FourLetterCommandHandler(stats, config);

        final ExecutorService threadPool =
                Executors.newFixedThreadPool(config.getInt("threads"));

        return new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_RCVBUF,
                        config.getInt("SO_RCVBUF"))
                .option(ChannelOption.SO_SNDBUF,
                        config.getInt("SO_SNDBUF"))
                .option(ChannelOption.RCVBUF_ALLOCATOR,
                        new FixedRecvByteBufAllocator(config.getInt("RECV_SIZE")))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel channel) throws Exception {
                        final ChannelPipeline pipeline = channel.pipeline();
                        pipeline
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
                                .addLast(flch);
                        finalizePipeline(pipeline);
                    }
                })
                .bind(new InetSocketAddress(config.getString("host"), config.getInt("port")));
    }
}
