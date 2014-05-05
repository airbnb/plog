package com.airbnb.plog.fragmentation;

import com.airbnb.plog.Tagged;
import com.airbnb.plog.stats.StatisticsReporter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DefaultByteBufHolder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.Collection;

@Slf4j
@ToString
public class FragmentedMessage extends DefaultByteBufHolder implements Tagged {
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
    @Getter
    private Collection<String> tags;

    private FragmentedMessage(ByteBufAllocator alloc,
                              final int totalLength,
                              final int fragmentCount,
                              final int fragmentSize,
                              final int hash) {
        super(alloc.buffer(totalLength, totalLength));
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
        final ByteBuf fragmentTagsBuffer = fragment.getTagsBuffer();

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

        if (fragmentTagsBuffer != null)
            this.tags = fragment.getTags();

        // valid fragment
        synchronized (receivedFragments) {
            receivedFragments.set(fragmentIndex);
            if (receivedFragments.cardinality() == this.fragmentCount) {
                this.complete = true;
            }
        }
        content().setBytes(foffset, fragmentPayload, 0, lengthOfCurrentFragment);
    }

    public final ByteBuf getPayload() {
        if (!isComplete())
            throw new IllegalStateException("Incomplete");

        content().readerIndex(0);
        content().writerIndex(getContentLength());
        return content();
    }

    public final int getContentLength() {
        return content().capacity();
    }
}
