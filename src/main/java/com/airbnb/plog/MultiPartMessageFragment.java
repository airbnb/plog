package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.ByteOrder;

@ToString
@RequiredArgsConstructor
public class MultiPartMessageFragment {
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
    private final ByteBuf payload;

    static MultiPartMessageFragment fromDatagram(DatagramPacket packet) {
        final ByteBuf content = packet.content().order(ByteOrder.BIG_ENDIAN);
        final int length = content.readableBytes();
        if (length < HEADER_SIZE)
            throw new IllegalArgumentException("Packet too short: " + length + " bytes");

        final int fragmentCount = content.getUnsignedShort(2);
        final int fragmentIndex = content.getUnsignedShort(4);
        if (fragmentIndex > fragmentCount)
            throw new IllegalArgumentException("Index " + fragmentIndex + " < count " + fragmentCount);
        final int packetSize = Math.min(content.getUnsignedShort(6), content.readableBytes() - HEADER_SIZE);
        final int idRightPart = content.getInt(8);
        final int totalLength = content.getInt(12);
        final ByteBuf payload = content.slice(HEADER_SIZE, packetSize);

        final int port = packet.sender().getPort();
        return new MultiPartMessageFragment(
                fragmentCount, fragmentIndex, packetSize,
                (port << 32) + idRightPart,
                totalLength, payload);
    }

    boolean isAlone() {
        return fragmentCount == 1;
    }
}
