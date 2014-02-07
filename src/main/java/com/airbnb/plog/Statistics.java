package com.airbnb.plog;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public final class Statistics {
    private final AtomicInteger
            tcpMessages = new AtomicInteger(),
            udpSimpleMessages = new AtomicInteger(),
            udpInvalidVersion = new AtomicInteger(),
            v0InvalidType = new AtomicInteger(),
            unknownCommand = new AtomicInteger(),
            v0Commands = new AtomicInteger(),
            v0MultipartMessages = new AtomicInteger();
    private final AtomicIntegerArray v0FragmentsLogScale = new AtomicIntegerArray(Short.SIZE);

    public final int receivedTcpMessage() {
        return this.tcpMessages.incrementAndGet();
    }

    public final int receivedUdpSimpleMessage() {
        return this.udpSimpleMessages.incrementAndGet();
    }

    public final int receivedUdpInvalidVersion() {
        return this.udpInvalidVersion.incrementAndGet();
    }

    public final int receivedV0InvalidType() {
        return this.v0InvalidType.incrementAndGet();
    }

    public final int receivedV0Command() {
        return this.v0Commands.incrementAndGet();
    }

    public final int receivedUnknownCommand() {
        return this.unknownCommand.incrementAndGet();
    }

    public final int receivedV0MultipartMessage() {
        return this.v0MultipartMessages.incrementAndGet();
    }

    public final int receivedV0MultipartFragment(int index) {
        final int log2 = 31 - Integer.numberOfLeadingZeros(index);
        return v0FragmentsLogScale.incrementAndGet(log2);
    }

    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"tcpMessages\":");
        builder.append(this.tcpMessages.get());
        builder.append(", \"udpSimpleMessages\":");
        builder.append(this.udpSimpleMessages.get());
        builder.append(", \"udpInvalidVersion\":");
        builder.append(this.udpInvalidVersion.get());
        builder.append(", \"v0InvalidType\":");
        builder.append(this.v0InvalidType.get());
        builder.append(", \"unknownCommand\":");
        builder.append(this.unknownCommand.get());
        builder.append(", \"v0Commands\":");
        builder.append(this.v0Commands.get());
        builder.append(", \"v0MultipartMessages\":[");
        for (int i = 0; i < Short.SIZE - 2; i++) {
            builder.append(v0FragmentsLogScale.get(i));
            builder.append(',');
        }
        builder.append(v0FragmentsLogScale.get(Short.SIZE - 1));
        builder.append("]}");
        return builder.toString();
    }

    public final String toJSON() {
        return this.toString();
    }
}
