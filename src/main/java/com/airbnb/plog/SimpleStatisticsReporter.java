package com.airbnb.plog;

import com.yammer.metrics.core.Meter;
import kafka.producer.*;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

@ToString
@RequiredArgsConstructor
@Slf4j
public final class SimpleStatisticsReporter implements StatisticsReporter {
    private final AtomicLong
            tcpMessages = new AtomicLong(),
            udpSimpleMessages = new AtomicLong(),
            udpInvalidVersion = new AtomicLong(),
            v0InvalidType = new AtomicLong(),
            unknownCommand = new AtomicLong(),
            v0Commands = new AtomicLong(),
            v0MultipartMessages = new AtomicLong(),
            failedToSend = new AtomicLong(),
            exceptions = new AtomicLong();
    private final AtomicLongArray v0FragmentsLogScale = new AtomicLongArray(Short.SIZE);
    private final String kafkaClientId;

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
    public long failedToSend() {
        return this.failedToSend.incrementAndGet();
    }

    @Override
    public long exception() {
        return this.exceptions.incrementAndGet();
    }

    @Override
    public final long receivedV0MultipartFragment(int index) {
        final int log2 = Integer.SIZE - Integer.numberOfLeadingZeros(index);
        return v0FragmentsLogScale.incrementAndGet(log2);
    }

    public final String toJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"tcpMessages\":");
        builder.append(this.tcpMessages.get());
        builder.append(",\"udpSimpleMessages\":");
        builder.append(this.udpSimpleMessages.get());
        builder.append(",\"udpInvalidVersion\":");
        builder.append(this.udpInvalidVersion.get());
        builder.append(",\"v0InvalidType\":");
        builder.append(this.v0InvalidType.get());
        builder.append(",\"unknownCommand\":");
        builder.append(this.unknownCommand.get());
        builder.append(",\"v0Commands\":");
        builder.append(this.v0Commands.get());
        builder.append(",\"v0MultipartMessages\":[");
        for (int i = 0; i < Short.SIZE - 2; i++) {
            builder.append(v0FragmentsLogScale.get(i));
            builder.append(',');
        }
        builder.append(v0FragmentsLogScale.get(Short.SIZE - 1));
        builder.append("],\"failedToSend\":");
        builder.append(this.failedToSend.get());
        builder.append(",\"exceptions\":");
        builder.append(this.exceptions.get());
        builder.append(",\"kafka\":{");

        final ProducerStats producerStats = ProducerStatsRegistry.getProducerStats(kafkaClientId);
        final ProducerTopicStats producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(kafkaClientId);
        final ProducerTopicMetrics producerAllTopicsStats = producerTopicStats.getProducerAllTopicsStats();

        builder.append("\"serializationErrorRate\":{");
        report(producerStats.serializationErrorRate(), builder);
        builder.append("},\"failedSendRate\":{");
        report(producerStats.failedSendRate(), builder);
        builder.append("},\"resendRate\":{");
        report(producerStats.resendRate(), builder);
        builder.append("},\"byteRate\":{");
        report(producerAllTopicsStats.byteRate(), builder);
        builder.append("},\"droppedMessageRate\":{");
        report(producerAllTopicsStats.droppedMessageRate(), builder);
        builder.append("},\"messageRate\":{");
        report(producerAllTopicsStats.messageRate(), builder);
        builder.append("}}}");

        return builder.toString();
    }

    private void report(Meter meter, StringBuilder builder) {
        builder.append("\"count\":");
        builder.append(meter.count());
        builder.append(",\"rate\":[");
        builder.append(meter.oneMinuteRate());
        builder.append(',');
        builder.append(meter.fiveMinuteRate());
        builder.append(',');
        builder.append(meter.fifteenMinuteRate());
        builder.append(']');
    }
}
