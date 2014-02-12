package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;

@Slf4j
public class PartialMultiPartMessage {
    private final ByteBuf payload;
    @Getter
    private final BitSet receivedFragments;
    @Getter
    private final int fragmentCount;
    @Getter
    private final int fragmentSize;
    @Getter
    private final int hash;
    @Getter
    private boolean complete = false;

    private PartialMultiPartMessage(final int totalLength,
                                    final int fragmentCount,
                                    final int fragmentSize,
                                    final int hash) {
        this.payload = Unpooled.buffer(totalLength, totalLength);
        this.payload.writerIndex(totalLength);
        this.receivedFragments = new BitSet(fragmentCount);
        this.fragmentCount = fragmentCount;
        this.fragmentSize = fragmentSize;
        this.hash = hash;
    }

    public static PartialMultiPartMessage fromFragment(final MultiPartMessageFragment fragment, StatisticsReporter stats) {
        final PartialMultiPartMessage msg = new PartialMultiPartMessage(
                fragment.getTotalLength(),
                fragment.getFragmentCount(),
                fragment.getFragmentSize(),
                fragment.getMsgHash());
        msg.ingestFragment(fragment, false, stats);
        return msg;
    }

    public void ingestFragment(final MultiPartMessageFragment fragment, StatisticsReporter stats) {
        ingestFragment(fragment, true, stats);
    }

    public void ingestFragment(final MultiPartMessageFragment fragment, boolean shouldCheck, StatisticsReporter stats) {
        final int fragmentSize = fragment.getFragmentSize();
        final int fragmentCount = fragment.getFragmentCount();
        final int msgHash = fragment.getMsgHash();
        final ByteBuf fragmentPayload = fragment.getPayload();
        final int fragmentIndex = fragment.getFragmentIndex();

        if ((this.getFragmentSize() != fragmentSize) ||
                this.getFragmentCount() != fragmentCount ||
                this.getHash() != msgHash) {
            log.warn("Invalid fragment {} for multipart {}", fragment, this);
            stats.receivedV0InvalidMultipartFragment(fragmentIndex, this.getFragmentCount());
        } else {
            // valid fragment
            final int foffset = fragmentSize * fragmentIndex;
            final int lengthFromFragment = Math.min(fragmentPayload.readableBytes(), fragmentSize);
            final int lengthToCopy = Math.min(lengthFromFragment, payload.capacity() - foffset);

            synchronized (receivedFragments) {
                receivedFragments.set(fragmentIndex);
                if (receivedFragments.cardinality() == this.fragmentCount) {
                    this.complete = true;
                }
            }
            payload.setBytes(foffset, fragmentPayload, 0, lengthToCopy);
            fragment.getPayload().release();
        }
    }

    public ByteBuf getPayload() {
        if (isComplete())
            return payload;
        else
            throw new IllegalStateException("Not complete");
    }

    public int length() {
        return payload.capacity();
    }
}
