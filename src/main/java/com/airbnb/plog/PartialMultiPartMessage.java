package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;

@Slf4j
public class PartialMultiPartMessage {
    private final ByteBuf payload;
    private final BitSet receivedFragments;
    private final int expectedFragments;
    @Getter
    private boolean complete = false;

    private PartialMultiPartMessage(int totalLength, int fragmentCount) {
        this.payload = Unpooled.buffer(totalLength, totalLength);
        this.payload.writerIndex(totalLength);
        receivedFragments = new BitSet(fragmentCount + 1);
        expectedFragments = fragmentCount + 1;
    }

    public static PartialMultiPartMessage fromFragment(MultiPartMessageFragment fragment) {
        final PartialMultiPartMessage msg = new PartialMultiPartMessage(
                fragment.getTotalLength(),
                fragment.getFragmentCount());
        msg.ingestFragment(fragment);
        return msg;
    }

    public void ingestFragment(MultiPartMessageFragment fragment) {
        final int size = fragment.getFragmentSize();
        final ByteBuf fpayload = fragment.getPayload();
        final int index = fragment.getFragmentIndex();
        final int foffset = size * index;
        final int copiedLength = Math.min(
                Math.min(fpayload.readableBytes(), size),
                payload.capacity() - foffset);
        synchronized (receivedFragments) {
            receivedFragments.set(index);
            if (receivedFragments.cardinality() == expectedFragments) {
                this.complete = true;
            }
        }
        payload.setBytes(foffset, fpayload, 0, copiedLength);
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
