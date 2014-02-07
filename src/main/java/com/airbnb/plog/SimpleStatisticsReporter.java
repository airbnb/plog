package com.airbnb.plog;

import lombok.ToString;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

@ToString
public final class SimpleStatisticsReporter implements StatisticsReporter {
    private final AtomicLong
            tcpMessages = new AtomicLong(),
            udpSimpleMessages = new AtomicLong(),
            udpInvalidVersion = new AtomicLong(),
            v0InvalidType = new AtomicLong(),
            unknownCommand = new AtomicLong(),
            v0Commands = new AtomicLong(),
            v0MultipartMessages = new AtomicLong();
    private final AtomicLongArray v0FragmentsLogScale = new AtomicLongArray(Short.SIZE);

    @Override
    public final long receivedTcpMessage() {
        return this.tcpMessages.incrementAndGet();
    }

    @Override
    public final long receivedUdpSimpleMessage() {
        return this.udpSimpleMessages.incrementAndGet();
    }

    @Override
    public final long receivedUdpInvalidVersion() {
        return this.udpInvalidVersion.incrementAndGet();
    }

    @Override
    public final long receivedV0InvalidType() {
        return this.v0InvalidType.incrementAndGet();
    }

    @Override
    public final long receivedV0Command() {
        return this.v0Commands.incrementAndGet();
    }

    @Override
    public final long receivedUnknownCommand() {
        return this.unknownCommand.incrementAndGet();
    }

    @Override
    public final long receivedV0MultipartMessage() {
        return this.v0MultipartMessages.incrementAndGet();
    }

    @Override
    public final long receivedV0MultipartFragment(int index) {
        final int log2 = 31 - Integer.numberOfLeadingZeros(index);
        return v0FragmentsLogScale.incrementAndGet(log2);
    }

    public final String toJSON() {
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
}
