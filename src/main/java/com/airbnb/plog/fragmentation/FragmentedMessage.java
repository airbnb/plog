package com.airbnb.plog.fragmentation;

import com.airbnb.plog.stats.StatisticsReporter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

    private FragmentedMessage(final int totalLength,
                              final int fragmentCount,
                              final int fragmentSize,
                              final int hash) {
        this.payload = Unpooled.buffer(totalLength, totalLength);
        this.payload.writerIndex(totalLength);
        this.receivedFragments = new BitSet(fragmentCount);
        this.fragmentCount = fragmentCount;
        this.fragmentSize = fragmentSize;
        this.checksum = hash;
    }

    public static FragmentedMessage fromFragment(final FragmentedMessageFragment fragment, StatisticsReporter stats) {
        final FragmentedMessage msg = new FragmentedMessage(
                fragment.getTotalLength(),
                fragment.getFragmentCount(),
                fragment.getFragmentSize(),
                fragment.getMsgHash());
        msg.ingestFragment(fragment, false, stats);
        return msg;
    }

    public void ingestFragment(final FragmentedMessageFragment fragment, StatisticsReporter stats) {
        ingestFragment(fragment, true, stats);
    }

    public void ingestFragment(final FragmentedMessageFragment fragment, boolean shouldCheck, StatisticsReporter stats) {
        final int fragmentSize = fragment.getFragmentSize();
        final int fragmentCount = fragment.getFragmentCount();
        final int msgHash = fragment.getMsgHash();
        final ByteBuf fragmentPayload = fragment.getPayload();
        final int fragmentIndex = fragment.getFragmentIndex();
        final boolean fragmentIsLast = fragmentIndex == fragmentCount - 1;
        final int foffset = fragmentSize * fragmentIndex;

        final int fragmentLength;
        final boolean validFragmentLength;

        if (fragmentIsLast) {
            fragmentLength = fragmentPayload.capacity();
            validFragmentLength = this.getPayloadLength() - foffset == fragmentLength;
        } else {
            fragmentLength = fragmentSize;
            validFragmentLength = fragmentLength == this.fragmentSize;
        }

        if (this.getFragmentSize() != fragmentSize ||
                this.getFragmentCount() != fragmentCount ||
                this.getChecksum() != msgHash ||
                !validFragmentLength) {
            FragmentedMessage.log.warn("Invalid {} for {}", fragment, this);
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
        payload.setBytes(foffset, fragmentPayload, 0, fragmentLength);
        fragment.getPayload().release();
    }

    public ByteBuf getPayload() {
        if (isComplete())
            return payload;
        else
            throw new IllegalStateException("Not complete");
    }

    public int getPayloadLength() {
        return payload.capacity();
    }
}
