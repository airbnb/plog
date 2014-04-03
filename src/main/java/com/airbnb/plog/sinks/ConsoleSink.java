package com.airbnb.plog.sinks;

import com.airbnb.plog.Message;
import io.netty.channel.ChannelHandlerContext;

public class ConsoleSink extends Sink {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        System.out.println(new String(msg.asBytes()));
    }
}
