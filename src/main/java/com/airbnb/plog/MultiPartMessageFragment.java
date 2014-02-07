package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;

import java.nio.ByteOrder;
import java.util.Arrays;

public class MultiPartMessageFragment {
    final int fragmentCount;
    final int fragmentIndex;
    final int fragmentSize;
    final int msgPort;
    final int msgId;
    final int totalLength;
    final byte[] payload;

    private MultiPartMessageFragment(int fragmentCount,
                                     int fragmentIndex,
                                     int fragmentSize,
                                     int msgPort,
                                     int msgId,
                                     int totalLength,
                                     byte[] payload) {
        this.fragmentCount = fragmentCount;
        this.fragmentIndex = fragmentIndex;
        this.fragmentSize = fragmentSize;
        this.msgPort = msgPort;
        this.msgId = msgId;
        this.totalLength = totalLength;
        this.payload = payload;
    }

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

    @Override
    public String toString() {
        return "MultiPartMessageFragment{" +
                "fragmentCount=" + fragmentCount +
                ", fragmentIndex=" + fragmentIndex +
                ", fragmentSize=" + fragmentSize +
                ", msgPort=" + msgPort +
                ", msgId=" + msgId +
                ", totalLength=" + totalLength +
                ", payload=" + Arrays.toString(payload) +
                '}';
    }

    public int getFragmentCount() {
        return fragmentCount;
    }

    public int getFragmentIndex() {
        return fragmentIndex;
    }

    public int getFragmentSize() {
        return fragmentSize;
    }

    public int getMsgPort() {
        return msgPort;
    }

    public int getMsgId() {
        return msgId;
    }

    public int getTotalLength() {
        return totalLength;
    }

    public byte[] getPayload() {
        return payload;
    }
}
