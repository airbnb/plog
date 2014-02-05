package com.airbnb.plog;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class PlogDefragmenter extends MessageToMessageDecoder<MultiPartMessageFragment> {
    private final Statistics stats;

    public PlogDefragmenter(Statistics stats) {
        this.stats = stats;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, MultiPartMessageFragment msg, List<Object> out) throws Exception {
        /* TODO: defragment */
    }
}
