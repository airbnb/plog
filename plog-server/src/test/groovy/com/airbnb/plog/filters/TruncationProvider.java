package com.airbnb.plog.filters;

import com.airbnb.plog.Message;
import com.airbnb.plog.MessageImpl;
import com.eclipsesource.json.JsonObject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TruncationProvider implements FilterProvider {
    @Override
    public Filter getFilter(Config config) throws Exception {
        final int maxLength = config.getInt("max_length");

        return new MessageSimpleChannelInboundHandler(maxLength);
    }

    private static class MessageSimpleChannelInboundHandler extends SimpleChannelInboundHandler<Message> implements Filter {
        private final int maxLength;

        public MessageSimpleChannelInboundHandler(int maxLength) {
            this.maxLength = maxLength;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
            final ByteBuf orig = msg.content();
            final int length = orig.readableBytes();

            if (length <= maxLength)
                ctx.fireChannelRead(msg);
            else
                ctx.fireChannelRead(new MessageImpl(msg.content().slice(0, maxLength)));
        }

        @Override
        public JsonObject getStats() {
            return null;
        }
    }
}
