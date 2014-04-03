package com.airbnb.plog;

import com.airbnb.plog.utils.ByteBufs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.Data;

import java.util.List;

@Data
public class Message extends DefaultByteBufHolder {
    public Message(ByteBuf data) {
        super(data);
    }

    public static Message fromBytes(ByteBufAllocator alloc, byte[] bytes) {
        final ByteBuf data = alloc.buffer(bytes.length, bytes.length);
        data.writeBytes(bytes);
        return new Message(data);
    }

    public byte[] asBytes() {
        return ByteBufs.toByteArray(content());
    }

    @ChannelHandler.Sharable
    public static final class ByteBufToMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
            buf.retain();
            out.add(new Message(buf));
        }
    }
}
