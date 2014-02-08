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
        if (length < 24)
            throw new IllegalArgumentException("Packet too short: " + length + " bytes");

        final int fragmentCount = content.getUnsignedShort(2);
        final int fragmentIndex = content.getUnsignedShort(4);
        if (fragmentIndex > fragmentCount)
            throw new IllegalArgumentException("Index " + fragmentIndex + " < count " + fragmentCount);
        final int packetSize = content.getUnsignedShort(6);
        final int idRightPart = content.getInt(8);
        final int totalLength = content.getInt(12);
        final ByteBuf payload = content.slice(24, packetSize);

        final int port = packet.sender().getPort();
        return new MultiPartMessageFragment(
                fragmentCount, fragmentIndex, packetSize,
                (port << 32) + idRightPart,
                totalLength, payload);
    }

    boolean isAlone() {
        return fragmentCount == 0;
    }
}
