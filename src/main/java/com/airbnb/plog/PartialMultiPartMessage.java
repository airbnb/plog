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
    private boolean complete = false;

    private PartialMultiPartMessage(final int totalLength, final int fragmentCount) {
        this.payload = Unpooled.buffer(totalLength, totalLength);
        this.payload.writerIndex(totalLength);
        this.receivedFragments = new BitSet(fragmentCount);
        this.fragmentCount = fragmentCount;
    }

    public static PartialMultiPartMessage fromFragment(final MultiPartMessageFragment fragment) {
        final PartialMultiPartMessage msg = new PartialMultiPartMessage(
                fragment.getTotalLength(),
                fragment.getFragmentCount());
        msg.ingestFragment(fragment);
        return msg;
    }

    public void ingestFragment(final MultiPartMessageFragment fragment) {
        final int size = fragment.getFragmentSize();
        final ByteBuf fpayload = fragment.getPayload();
        final int index = fragment.getFragmentIndex();
        final int foffset = size * index;
        final int lengthFromFragment = Math.min(fpayload.readableBytes(), size);
        final int lengthToCopy = Math.min(lengthFromFragment, payload.capacity() - foffset);
        synchronized (receivedFragments) {
            receivedFragments.set(index);
            if (receivedFragments.cardinality() == fragmentCount) {
                this.complete = true;
            }
        }
        payload.setBytes(foffset, fpayload, 0, lengthToCopy);
        fragment.getPayload().release();
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
