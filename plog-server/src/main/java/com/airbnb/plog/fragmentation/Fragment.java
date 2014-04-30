package com.airbnb.plog.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;

import java.nio.ByteOrder;

public class Fragment extends DefaultByteBufHolder {
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

    public Fragment(int fragmentCount, int fragmentIndex, int fragmentSize, long msgId, int totalLength, int msgHash, ByteBuf data) {
        super(data);
        this.fragmentCount = fragmentCount;
        this.fragmentIndex = fragmentIndex;
        this.fragmentSize = fragmentSize;
        this.msgId = msgId;
        this.totalLength = totalLength;
        this.msgHash = msgHash;
    }

    public static Fragment fromDatagram(DatagramPacket packet) {
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
        final ByteBuf payload = content.slice(HEADER_SIZE, length - HEADER_SIZE);

        final int port = packet.sender().getPort();
        final long msgId = (((long) port) << Integer.SIZE) + idRightPart;
        return new Fragment(fragmentCount, fragmentIndex, fragmentSize, msgId, totalLength, msgHash, payload);
    }

    boolean isAlone() {
        return fragmentCount == 1;
    }
}
