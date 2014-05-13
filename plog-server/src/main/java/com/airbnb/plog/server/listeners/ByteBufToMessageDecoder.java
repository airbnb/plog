package com.airbnb.plog.server.listeners;

import com.airbnb.plog.MessageImpl;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

@ChannelHandler.Sharable
final class ByteBufToMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
        buf.retain();
        out.add(new MessageImpl(buf, null));
    }
}
