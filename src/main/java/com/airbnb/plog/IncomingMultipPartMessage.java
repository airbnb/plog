package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.BitSet;

public class IncomingMultipPartMessage {
    private final ByteBuf payload;
    private final BitSet receivedFragments;
    private boolean ready = false;

    private IncomingMultipPartMessage(int totalLength, int fragmentCount) {
        this.payload = Unpooled.buffer(totalLength, totalLength);
        receivedFragments = new BitSet(fragmentCount);
    }

    public IncomingMultipPartMessage fromFragment(MultiPartMessageFragment fragment) {
        final IncomingMultipPartMessage msg =
                new IncomingMultipPartMessage(fragment.getTotalLength(), fragment.getFragmentCount());
        msg.ingestFragment(fragment);
        return msg;
    }

    private void ingestFragment(MultiPartMessageFragment fragment) {
        final int size = fragment.getFragmentSize();
        final int index = fragment.getFragmentIndex();
        synchronized (receivedFragments) {
            receivedFragments.set(index);
            if (receivedFragments.cardinality() == receivedFragments.size())
                ready = true;
        }
        payload.writeBytes(fragment.getPayload(), size * index, size);
    }

    public boolean isReady() {
        return ready;
    }

    public ByteBuf getPayload() {
        if (isReady())
            return payload;
        else
            throw new IllegalStateException("Not ready");
    }
}
