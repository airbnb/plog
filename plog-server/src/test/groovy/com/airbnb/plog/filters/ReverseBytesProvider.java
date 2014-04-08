package com.airbnb.plog.filters;

import com.airbnb.plog.Message;
import com.airbnb.plog.MessageImpl;
import com.eclipsesource.json.JsonObject;
import com.typesafe.config.Config;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ReverseBytesProvider implements FilterProvider {
    @Override
    public Filter getFilter(Config config) throws Exception {
        return new ReverseBytesFilter();
    }

    private static class ReverseBytesFilter extends SimpleChannelInboundHandler<Message> implements Filter {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
            final byte[] payload = msg.asBytes();
            final int length = payload.length;

            final byte[] reverse = new byte[length];
            for (int i = 0; i < length; i++)
                reverse[i] = payload[length - i - 1];

            ctx.fireChannelRead(MessageImpl.fromBytes(ctx.alloc(), reverse));
        }

        @Override
        public JsonObject getStats() {
            return null;
        }
    }
}
