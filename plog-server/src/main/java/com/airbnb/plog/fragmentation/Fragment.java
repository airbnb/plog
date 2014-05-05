package com.airbnb.plog.fragmentation;

import com.airbnb.plog.Tagged;
import com.airbnb.plog.utils.ByteBufs;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.socket.DatagramPacket;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;

import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;

@ToString(exclude = {"tagsBuffer"})
public class Fragment extends DefaultByteBufHolder implements Tagged {
    static final int HEADER_SIZE = 24;

    @Getter
    private final int fragmentCount;
    @Getter
    private final int fragmentIndex;
    @Getter
    private final int fragmentSize;
    @Getter
    private final long msgId;
    @Getter
    private final int totalLength;
    @Getter
    private final int msgHash;

    @Getter(AccessLevel.MODULE)
    private final ByteBuf tagsBuffer;

    public Fragment(int fragmentCount,
                    int fragmentIndex,
                    int fragmentSize,
                    long msgId,
                    int totalLength,
                    int msgHash,
                    ByteBuf data,
                    ByteBuf tagsBuffer) {
        super(data);
        this.fragmentCount = fragmentCount;
        this.fragmentIndex = fragmentIndex;
        this.fragmentSize = fragmentSize;
        this.msgId = msgId;
        this.totalLength = totalLength;
        this.msgHash = msgHash;
        this.tagsBuffer = tagsBuffer;
    }

    public static Fragment fromDatagram(DatagramPacket packet) {
        packet.content().retain();
        final ByteBuf content = packet.content().order(ByteOrder.BIG_ENDIAN);

        final int length = content.readableBytes();
        if (length < HEADER_SIZE)
            throw new IllegalArgumentException("Packet too short: " + length + " bytes");

        final int fragmentCount = content.getUnsignedShort(2);
        if (fragmentCount == 0)
            throw new IllegalArgumentException("0 fragment count");

        final int fragmentIndex = content.getUnsignedShort(4);
        if (fragmentIndex >= fragmentCount)
            throw new IllegalArgumentException("Index " + fragmentIndex + " < count " + fragmentCount);

        final int fragmentSize = content.getUnsignedShort(6);
        final int idRightPart = content.getInt(8);
        final int totalLength = content.getInt(12);
        if (totalLength < 0)
            throw new IllegalArgumentException("Cannot support length " + totalLength + " > 2^31");

        final int msgHash = content.getInt(16);

        final int tagsBufferLength = content.getUnsignedShort(20);
        final ByteBuf tagsBuffer = tagsBufferLength == 0 ? null : content.slice(HEADER_SIZE, tagsBufferLength);

        final ByteBuf payload = content.slice(HEADER_SIZE + tagsBufferLength, length - HEADER_SIZE - tagsBufferLength);

        final int port = packet.sender().getPort();
        final long msgId = (((long) port) << Integer.SIZE) + idRightPart;

        return new Fragment(fragmentCount, fragmentIndex, fragmentSize, msgId, totalLength, msgHash, payload, tagsBuffer);
    }

    boolean isAlone() {
        return fragmentCount == 1;
    }

    @Override
    public Collection<String> getTags() {
        if (tagsBuffer == null)
            return Collections.emptyList();
        final String seq = new String(ByteBufs.toByteArray(tagsBuffer), Charsets.UTF_8);
        return Splitter.on('\0').omitEmptyStrings().splitToList(seq);
    }
}
