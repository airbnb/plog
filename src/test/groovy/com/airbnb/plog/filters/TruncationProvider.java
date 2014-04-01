package com.airbnb.plog.filters;

import com.airbnb.plog.Message;
import com.typesafe.config.Config;
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
                final byte[] payload = msg.getPayload();
                final int originalLength = payload.length;
                final int truncatedLength = Integer.min(originalLength, maxLength);

                final byte[] truncated = new byte[truncatedLength];
                System.arraycopy(payload, 0, truncated, 0, truncatedLength);

                ctx.fireChannelRead(new Message(truncated));
            }
        };
    }
}
