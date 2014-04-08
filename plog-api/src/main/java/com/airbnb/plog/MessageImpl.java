package com.airbnb.plog;

import com.airbnb.plog.utils.ByteBufs;
import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DefaultByteBufHolder;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Data
@EqualsAndHashCode(callSuper = false)
public class MessageImpl extends DefaultByteBufHolder implements Message {
    private final byte[][] tags;

    @Getter(AccessLevel.NONE)
    private byte[] memoizedBytes;

    public MessageImpl(ByteBuf data, byte[][] tags) {
        super(data);
        this.tags = tags;
    }

    public MessageImpl(ByteBuf data) {
        super(data);
        tags = null;
    }

    public static Message fromBytes(ByteBufAllocator alloc, byte[] bytes) {
        final ByteBuf data = alloc.buffer(bytes.length, bytes.length);
        data.writeBytes(bytes);
        return new MessageImpl(data);
    }

    @Override
    public byte[] asBytes() {
        if (memoizedBytes == null)
            memoizedBytes = ByteBufs.toByteArray(content());

        return memoizedBytes;
    }

    @Override
    public final String toString() {
        if (tags == null) {
            return new String(asBytes());
        } else {
            final String tagList = Joiner.on(',').join(tags);
            return "[" + tagList + "] " + new String(asBytes());
        }
    }
}
