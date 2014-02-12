package com.airbnb.plog;

import com.google.common.cache.CacheStats;
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
            udpSimpleMessages = new AtomicLong(),
            udpInvalidVersion = new AtomicLong(),
            v0InvalidType = new AtomicLong(),
            unknownCommand = new AtomicLong(),
            v0Commands = new AtomicLong(),
            v0MultipartMessages = new AtomicLong(),
            failedToSend = new AtomicLong(),
            exceptions = new AtomicLong();
    private final AtomicLongArray
            v0MultipartMessageFragments = new AtomicLongArray(Short.SIZE),
            v0InvalidChecksum = new AtomicLongArray(Short.SIZE),
            droppedFragments = new AtomicLongArray(Short.SIZE * Short.SIZE),
            invalidFragments = new AtomicLongArray(Short.SIZE * Short.SIZE);
    private final String kafkaClientId;
    private CacheStats cacheStats = null;

    private static final int intLog2(int i) {
        return Integer.SIZE - Integer.numberOfLeadingZeros(i);
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
    public final long receivedV0MultipartFragment(final int index) {
        return v0MultipartMessageFragments.incrementAndGet(intLog2(index));
    }

    @Override
    public final long receivedV0InvalidChecksum(int fragments) {
        return this.v0InvalidChecksum.incrementAndGet(fragments - 1);
    }

    @Override
    public long receivedV0InvalidMultipartFragment(final int fragmentIndex, final int expectedFragments) {
        final int target = (Short.SIZE * intLog2(expectedFragments - 1)) + intLog2(fragmentIndex);
        return droppedFragments.incrementAndGet(target);
    }

    @Override
    public long missingFragmentInDroppedMultiPartMessage(final int fragmentIndex, final int expectedFragments) {
        final int target = (Short.SIZE * intLog2(expectedFragments - 1)) + intLog2(fragmentIndex);
        return droppedFragments.incrementAndGet(target);
    }

    public final String toJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"udpSimpleMessages\":");
        builder.append(this.udpSimpleMessages.get());
        builder.append(",\"udpInvalidVersion\":");
        builder.append(this.udpInvalidVersion.get());
        builder.append(",\"v0InvalidType\":");
        builder.append(this.v0InvalidType.get());
        builder.append(",\"unknownCommand\":");
        builder.append(this.unknownCommand.get());
        builder.append(",\"v0Commands\":");
        builder.append(this.v0Commands.get());

        builder.append(',');
        appendLogStats(builder, "v0MultipartMessageFragments", v0MultipartMessageFragments);
        builder.append(',');
        appendLogStats(builder, "v0InvalidChecksum", v0InvalidChecksum);
        builder.append(',');

        appendLogLogStats(builder, "v0InvalidFragments", invalidFragments);
        builder.append(',');
        appendLogLogStats(builder, "missingFragmentsInDroppedMultipartMessages", droppedFragments);

        builder.append(",\"failedToSend\":");
        builder.append(this.failedToSend.get());
        builder.append(",\"exceptions\":");
        builder.append(this.exceptions.get());

        if (cacheStats != null) {
            builder.append(",\"cache\":{\"evictions\":");
            builder.append(cacheStats.evictionCount());
            builder.append(",\"hitCount\":");
            builder.append(cacheStats.hitCount());
            builder.append(",\"missCount\":");
            builder.append(cacheStats.missCount());
            builder.append('}');
        }

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

    private void appendLogStats(StringBuilder builder, String name, AtomicLongArray data) {
        builder.append('\"');
        builder.append(name);
        builder.append("\":[");
        for (int i = 0; i < Short.SIZE - 1; i++) {
            builder.append(data.get(i));
            builder.append(',');
        }
        builder.append(data.get(Short.SIZE - 1));
        builder.append(']');
    }

    private void appendLogLogStats(StringBuilder builder, String name, AtomicLongArray data) {
        builder.append('\"');
        builder.append(name);
        builder.append("\":[");
        for (int packetCountLog = 0; packetCountLog < Short.SIZE; packetCountLog++) {
            builder.append('[');
            for (int packetIndexLog = 0; packetIndexLog < Short.SIZE; packetIndexLog++) {
                builder.append(data.get(packetCountLog * Short.SIZE + packetIndexLog));

                if (packetIndexLog != Short.SIZE - 1)
                    builder.append(',');
            }
            builder.append(']');

            if (packetCountLog != Short.SIZE - 1)
                builder.append(',');
        }
        builder.append(']');
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

    public synchronized void withDefragCacheStats(CacheStats cacheStats) {
        if (this.cacheStats == null)
            this.cacheStats = cacheStats;
        else
            throw new IllegalStateException("Cache stat already provided");
    }
}
