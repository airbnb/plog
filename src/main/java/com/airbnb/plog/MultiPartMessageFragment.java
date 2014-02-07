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
    final int fragmentCount;
    @Getter
    final int fragmentIndex;
    @Getter
    final int fragmentSize;
    @Getter
    final int msgPort;
    @Getter
    final int msgId;
    @Getter
    final int totalLength;
    @Getter
    final byte[] payload;

    static MultiPartMessageFragment fromDatagram(DatagramPacket packet) {
        final ByteBuf content = packet.content().order(ByteOrder.LITTLE_ENDIAN);
        final int length = content.readableBytes();
        if (length < 24)
            throw new IllegalArgumentException("Packet too short: " + length + " bytes");

        final int packetCount = content.getUnsignedShort(2);
        final int packetIndex = content.getUnsignedShort(4);
        final int packetSize = content.getUnsignedShort(6);
        final int msgId = content.getInt(8);
        final int totalLength = content.getInt(12);
        final byte[] payload = new byte[packetSize];
        content.getBytes(24, payload, 0, packetSize);

        final int msgPort = packet.sender().getPort();
        return new MultiPartMessageFragment(packetCount, packetIndex, packetSize, msgPort, msgId, totalLength, payload);
    }
}
