package com.airbnb.plog.server.listeners;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import lombok.Data;

@Data
final class StartReturn {
    private final ChannelFuture bindFuture;
    private final EventLoopGroup eventLoopGroup;
}
