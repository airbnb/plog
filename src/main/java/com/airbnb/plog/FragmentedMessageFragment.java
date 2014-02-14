package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.ByteOrder;

@ToString(exclude = {"payload"})
@RequiredArgsConstructor
public class FragmentedMessageFragment {
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
    @Getter
    private final ByteBuf payload;

    static FragmentedMessageFragment fromDatagram(DatagramPacket packet) {
        final ByteBuf content = packet.content().order(ByteOrder.BIG_ENDIAN);
        final int length = content.readableBytes();
        if (length < HEADER_SIZE)
            throw new IllegalArgumentException("Packet too short: " + length + " bytes");

        final int fragmentCount = content.getUnsignedShort(2);
        final int fragmentIndex = content.getUnsignedShort(4);
        if (fragmentIndex > fragmentCount)
            throw new IllegalArgumentException("Index " + fragmentIndex + " < count " + fragmentCount);
        final int fragmentSize = content.getUnsignedShort(6);
        final int idRightPart = content.getInt(8);
        final int totalLength = content.getInt(12);
        final int msgHash = content.getInt(16);
        final ByteBuf payload = content.slice(HEADER_SIZE, length - HEADER_SIZE);

        final int port = packet.sender().getPort();
        final long msgId = (((long) port) << Integer.SIZE) + idRightPart;
        return new FragmentedMessageFragment(fragmentCount, fragmentIndex, fragmentSize, msgId, totalLength, msgHash, payload);
    }

    boolean isAlone() {
        return fragmentCount == 1;
    }
}
