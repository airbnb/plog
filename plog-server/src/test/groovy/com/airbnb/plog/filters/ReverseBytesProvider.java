package com.airbnb.plog.filters;

import com.airbnb.plog.Message;
import com.typesafe.config.Config;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ReverseBytesProvider implements FilterProvider {
    @Override
    public ChannelHandler getFilter(Config config) throws Exception {
        return new SimpleChannelInboundHandler<Message>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
                final byte[] payload = msg.asBytes();
                final int length = payload.length;

                final byte[] reverse = new byte[length];
                for (int i = 0; i < length; i++)
                    reverse[i] = payload[length - i - 1];

                ctx.fireChannelRead(Message.fromBytes(ctx.alloc(), reverse));
            }
        };
    }
}
