package com.airbnb.plog.stats;

import com.airbnb.plog.fragmentation.Defragmenter;
import com.google.common.cache.CacheStats;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Meter;
import kafka.producer.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

@RequiredArgsConstructor
@Slf4j
public final class SimpleStatisticsReporter implements StatisticsReporter {
    private final AtomicLong
            holesFromDeadPort = new AtomicLong(),
            holesFromNewMessage = new AtomicLong(),
            udpSimpleMessages = new AtomicLong(),
            udpInvalidVersion = new AtomicLong(),
            v0InvalidType = new AtomicLong(),
            unknownCommand = new AtomicLong(),
            v0Commands = new AtomicLong(),
            v0MultipartMessages = new AtomicLong(),
            failedToSend = new AtomicLong(),
            exceptions = new AtomicLong();
    private final AtomicLongArray
            v0MultipartMessageFragments = new AtomicLongArray(Short.SIZE + 1),
            v0InvalidChecksum = new AtomicLongArray(Short.SIZE + 1),
            droppedFragments = new AtomicLongArray((Short.SIZE + 1) * (Short.SIZE + 1)),
            invalidFragments = new AtomicLongArray((Short.SIZE + 1) * (Short.SIZE + 1));
    private final String kafkaClientId;
    private final long startTime = System.currentTimeMillis();
    private Set<Defragmenter> defragmenters = Sets.newHashSet();

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
    public long foundHolesFromDeadPort(int holesFound) {
        return holesFromDeadPort.addAndGet(holesFound);
    }

    @Override
    public long foundHolesFromNewMessage(int holesFound) {
        return holesFromNewMessage.addAndGet(holesFound);
    }

    @Override
    public final long receivedV0MultipartFragment(final int index) {
        return v0MultipartMessageFragments.incrementAndGet(intLog2(index));
    }

    @Override
    public final long receivedV0InvalidChecksum(int fragments) {
        return this.v0InvalidChecksum.incrementAndGet(intLog2(fragments - 1));
    }

    @Override
    public long receivedV0InvalidMultipartFragment(final int fragmentIndex, final int expectedFragments) {
        final int target = ((Short.SIZE + 1) * intLog2(expectedFragments - 1)) + intLog2(fragmentIndex);
        return invalidFragments.incrementAndGet(target);
    }

    @Override
    public long missingFragmentInDroppedMessage(final int fragmentIndex, final int expectedFragments) {
        final int target = ((Short.SIZE + 1) * intLog2(expectedFragments - 1)) + intLog2(fragmentIndex);
        return droppedFragments.incrementAndGet(target);
    }

    public final String toJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"uptime\":");
        builder.append(System.currentTimeMillis() - this.startTime);
        builder.append(",\"udp_simple_messages\":");
        builder.append(this.udpSimpleMessages.get());
        builder.append(",\"udp_invalid_version\":");
        builder.append(this.udpInvalidVersion.get());
        builder.append(",\"v0_invalid_type\":");
        builder.append(this.v0InvalidType.get());
        builder.append(",\"unknown_command\":");
        builder.append(this.unknownCommand.get());
        builder.append(",\"v0_commands\":");
        builder.append(this.v0Commands.get());
        builder.append(",\"failed_to_send\":");
        builder.append(this.failedToSend.get());
        builder.append(",\"exceptions\":");
        builder.append(this.exceptions.get());
        builder.append(",\"holes_from_dead_port\":");
        builder.append(this.holesFromDeadPort.get());
        builder.append(",\"holes_from_new_message\":");
        builder.append(this.holesFromNewMessage.get());

        builder.append(',');
        appendLogStats(builder, "v0_fragments", v0MultipartMessageFragments);
        builder.append(',');
        appendLogStats(builder, "v0_invalid_checksum", v0InvalidChecksum);
        builder.append(',');

        appendLogLogStats(builder, "v0_invalid_fragments", invalidFragments);
        builder.append(',');
        appendLogLogStats(builder, "dropped_fragments", droppedFragments);

        final CacheStats cacheStats = this.defragmentersStats();
        builder.append(",\"cache\":{\"evictions\":");
        builder.append(cacheStats.evictionCount());
        builder.append(",\"hits\":");
        builder.append(cacheStats.hitCount());
        builder.append(",\"misses\":");
        builder.append(cacheStats.missCount());
        builder.append('}');

        if (kafkaClientId != null)
            appendKafka(builder);

        builder.append("}");

        return builder.toString();
    }

    private void appendKafka(StringBuilder builder) {
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
        builder.append("}}");
    }

    private void appendLogStats(StringBuilder builder, String name, AtomicLongArray data) {
        builder.append('\"');
        builder.append(name);
        builder.append("\":[");
        for (int i = 0; i < data.length(); i++) {
            builder.append(data.get(i));
            builder.append(',');
        }
        builder.append(data.get(data.length() - 1));
        builder.append(']');
    }

    private void appendLogLogStats(StringBuilder builder, String name, AtomicLongArray data) {
        builder.append('\"');
        builder.append(name);
        builder.append("\":[");
        for (int packetCountLog = 0; packetCountLog <= Short.SIZE; packetCountLog++) {
            builder.append('[');
            for (int packetIndexLog = 0; packetIndexLog <= packetCountLog; packetIndexLog++) {
                builder.append(data.get(packetCountLog * (Short.SIZE + 1) + packetIndexLog));
                if (packetIndexLog != packetCountLog)
                    builder.append(',');
            }
            builder.append(']');

            if (packetCountLog != Short.SIZE)
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

    private CacheStats defragmentersStats() {
        long hitCount = 0, missCount = 0, loadSuccessCount = 0, loadExceptionCount = 0,
                totalLoadTime = 0, evictionCount = 0;
        for (Defragmenter defragmenter : this.defragmenters) {
            final CacheStats stats = defragmenter.getCacheStats();
            hitCount += stats.hitCount();
            missCount += stats.missCount();
            loadSuccessCount += stats.loadSuccessCount();
            loadExceptionCount += stats.loadExceptionCount();
            totalLoadTime += stats.totalLoadTime();
            evictionCount += stats.evictionCount();
        }
        return new CacheStats(
                hitCount, missCount, loadSuccessCount, loadExceptionCount,
                totalLoadTime, evictionCount);
    }

    public synchronized void withDefrag(Defragmenter defragmenter) {
        this.defragmenters.add(defragmenter);
    }
}
