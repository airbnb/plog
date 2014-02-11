package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.Data;

import java.util.List;

@Data
public class Message {
    private final ByteBuf payload;

    @ChannelHandler.Sharable
    public static final class ByteBufToMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
            buf.retain();
            out.add(new Message(buf));
        }
    }
}
