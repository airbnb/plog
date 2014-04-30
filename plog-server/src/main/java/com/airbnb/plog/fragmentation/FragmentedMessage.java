package com.airbnb.plog.fragmentation;

import com.airbnb.plog.stats.StatisticsReporter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;

@Slf4j
@ToString(exclude = {"payload"})
public class FragmentedMessage {
    private final ByteBuf payload;
    @Getter
    private final BitSet receivedFragments;
    @Getter
    private final int fragmentCount;
    @Getter
    private final int fragmentSize;
    @Getter
    private final int checksum;
    @Getter
    private boolean complete = false;

    private FragmentedMessage(ByteBufAllocator alloc,
                              final int totalLength,
                              final int fragmentCount,
                              final int fragmentSize,
                              final int hash) {
        this.payload = alloc.buffer(totalLength, totalLength);
        this.receivedFragments = new BitSet(fragmentCount);
        this.fragmentCount = fragmentCount;
        this.fragmentSize = fragmentSize;
        this.checksum = hash;
    }

    public static FragmentedMessage fromFragment(final Fragment fragment, StatisticsReporter stats) {
        final FragmentedMessage msg = new FragmentedMessage(
                fragment.content().alloc(),
                fragment.getTotalLength(),
                fragment.getFragmentCount(),
                fragment.getFragmentSize(),
                fragment.getMsgHash());
        msg.ingestFragment(fragment, stats);
        return msg;
    }

    public final void ingestFragment(final Fragment fragment, StatisticsReporter stats) {
        final int fragmentSize = fragment.getFragmentSize();
        final int fragmentCount = fragment.getFragmentCount();
        final int msgHash = fragment.getMsgHash();
        final ByteBuf fragmentPayload = fragment.content();
        final int fragmentIndex = fragment.getFragmentIndex();
        final boolean fragmentIsLast = (fragmentIndex == fragmentCount - 1);
        final int foffset = fragmentSize * fragmentIndex;

        final int lengthOfCurrentFragment = fragmentPayload.capacity();
        final boolean validFragmentLength;

        if (fragmentIsLast) {
            validFragmentLength = (lengthOfCurrentFragment == this.getContentLength() - foffset);
        } else {
            validFragmentLength = (lengthOfCurrentFragment == this.fragmentSize);
        }

        if (this.getFragmentSize() != fragmentSize ||
                this.getFragmentCount() != fragmentCount ||
                this.getChecksum() != msgHash ||
                !validFragmentLength) {
            log.warn("Invalid {} for {}", fragment, this);
            stats.receivedV0InvalidMultipartFragment(fragmentIndex, this.getFragmentCount());
            return;
        }

        // valid fragment
        synchronized (receivedFragments) {
            receivedFragments.set(fragmentIndex);
            if (receivedFragments.cardinality() == this.fragmentCount) {
                this.complete = true;
            }
        }
        payload.setBytes(foffset, fragmentPayload, 0, lengthOfCurrentFragment);
    }

    public final ByteBuf getPayload() {
        if (!isComplete())
            throw new IllegalStateException("Incomplete");

        payload.readerIndex(0);
        payload.writerIndex(getContentLength());
        return payload;
    }

    public final int getContentLength() {
        return payload.capacity();
    }
}
