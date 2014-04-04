package com.airbnb.plog.filters;

import com.airbnb.plog.Message;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TruncationProvider extends FilterProvider {
    @Override
    public ChannelHandler getFilter(Config config) throws Exception {
        final int maxLength = config.getInt("max_length");

        return new SimpleChannelInboundHandler<Message>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
                final ByteBuf orig = msg.content();
                final int length = orig.readableBytes();

                if (length <= maxLength)
                    ctx.fireChannelRead(msg);
                else
                    ctx.fireChannelRead(new Message(msg.content().slice(0, maxLength)));
            }
        };
    }
}
