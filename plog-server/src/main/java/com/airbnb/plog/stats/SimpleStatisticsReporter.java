package com.airbnb.plog.stats;

import com.airbnb.plog.fragmentation.Defragmenter;
import com.airbnb.plog.handlers.Handler;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.google.common.cache.CacheStats;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

@Slf4j
public final class SimpleStatisticsReporter implements StatisticsReporter {
    private final AtomicLong
            holesFromDeadPort = new AtomicLong(),
            holesFromNewMessage = new AtomicLong(),
            udpSimpleMessages = new AtomicLong(),
            udpInvalidVersion = new AtomicLong(),
            v0InvalidType = new AtomicLong(),
            v0InvalidMultipartHeader = new AtomicLong(),
            unknownCommand = new AtomicLong(),
            v0Commands = new AtomicLong(),
            v0MultipartMessages = new AtomicLong(),
            failedToSend = new AtomicLong(),
            exceptions = new AtomicLong(),
            unhandledObjects = new AtomicLong();
    private final AtomicLongArray
            v0MultipartMessageFragments = new AtomicLongArray(Short.SIZE + 1),
            v0InvalidChecksum = new AtomicLongArray(Short.SIZE + 1),
            droppedFragments = new AtomicLongArray((Short.SIZE + 1) * (Short.SIZE + 1)),
            invalidFragments = new AtomicLongArray((Short.SIZE + 1) * (Short.SIZE + 1));

    private final long startTime = System.currentTimeMillis();
    private String MEMOIZED_PLOG_VERSION = null;
    private Defragmenter defragmenter = null;
    private List<Handler> handlers = Lists.newArrayList();

    private static int intLog2(int i) {
        return Integer.SIZE - Integer.numberOfLeadingZeros(i);
    }

    private static JsonArray arrayForLogStats(AtomicLongArray data) {
        final JsonArray result = new JsonArray();
        for (int i = 0; i < data.length(); i++)
            result.add(data.get(i));
        return result;
    }

    private static JsonArray arrayForLogLogStats(AtomicLongArray data) {
        final JsonArray result = new JsonArray();
        for (int packetCountLog = 0; packetCountLog <= Short.SIZE; packetCountLog++) {
            final JsonArray entry = new JsonArray();
            result.add(entry);
            for (int packetIndexLog = 0; packetIndexLog <= packetCountLog; packetIndexLog++)
                entry.add(data.get(packetCountLog * (Short.SIZE + 1) + packetIndexLog));
        }
        return result;
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
    public final long receivedV0InvalidMultipartHeader() {
        return this.v0InvalidMultipartHeader.incrementAndGet();
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

    @Override
    public long unhandledObject() {
        return unhandledObjects.incrementAndGet();
    }

    public final String toJSON() {
        final JsonObject result = new JsonObject()
                .add("version", getPlogVersion())
                .add("uptime", System.currentTimeMillis() - startTime)
                .add("udp_simple_messages", udpSimpleMessages.get())
                .add("udp_invalid_version", udpInvalidVersion.get())
                .add("v0_invalid_type", v0InvalidType.get())
                .add("v0_invalid_multipart_header", v0InvalidMultipartHeader.get())
                .add("unknown_command", unknownCommand.get())
                .add("v0_commands", v0Commands.get())
                .add("failed_to_send", failedToSend.get())
                .add("exceptions", exceptions.get())
                .add("unhandled_objects", unhandledObjects.get())
                .add("holes_from_dead_port", holesFromDeadPort.get())
                .add("holes_from_new_message", holesFromNewMessage.get())
                .add("v0_fragments", arrayForLogStats(v0MultipartMessageFragments))
                .add("v0_invalid_checksum", arrayForLogStats(v0InvalidChecksum))
                .add("v0_invalid_fragments", arrayForLogLogStats(invalidFragments))
                .add("dropped_fragments", arrayForLogLogStats(droppedFragments));

        if (defragmenter != null) {
            final CacheStats cacheStats = defragmenter.getCacheStats();
            result.add("defragmenter", new JsonObject()
                    .add("evictions", cacheStats.evictionCount())
                    .add("hits", cacheStats.hitCount())
                    .add("misses", cacheStats.missCount()));
        }

        final JsonArray handlersStats = new JsonArray();
        result.add("handlers", handlersStats);
        for (Handler handler : handlers) {
            final JsonObject statsCandidate = handler.getStats();
            final JsonObject stats = (statsCandidate == null) ? new JsonObject() : statsCandidate;
            handlersStats.add(stats.set("name", handler.getName()));
        }

        return result.toString();
    }

    private String getPlogVersion() {
        if (MEMOIZED_PLOG_VERSION == null)
            try {
                MEMOIZED_PLOG_VERSION = readVersionFromManifest();
            } catch (Throwable e) {
                log.warn("Couldn't get version", e);
                MEMOIZED_PLOG_VERSION = "unknown";
            }
        return MEMOIZED_PLOG_VERSION;
    }

    private String readVersionFromManifest() throws IOException {
        final Enumeration<URL> resources = getClass().getClassLoader()
                .getResources(JarFile.MANIFEST_NAME);
        while (resources.hasMoreElements()) {
            final URL url = resources.nextElement();
            final Attributes mainAttributes = new Manifest(url.openStream()).getMainAttributes();
            final String version = mainAttributes.getValue("Plog-Version");
            if (version != null)
                return version;
        }
        throw new NoSuchFieldError();
    }

    public synchronized void withDefrag(Defragmenter defragmenter) {
        if (this.defragmenter == null)
            this.defragmenter = defragmenter;
        else
            throw new IllegalStateException("Defragmenter already provided!");
    }

    public synchronized void appendHandler(Handler handler) {
        this.handlers.add(handler);
    }
}
