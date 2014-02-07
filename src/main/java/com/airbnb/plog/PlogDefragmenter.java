package com.airbnb.plog;

import com.typesafe.config.Config;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class PlogDefragmenter extends SimpleChannelInboundHandler<MultiPartMessageFragment> {
    private final Statistics stats;

    public PlogDefragmenter(Statistics stats, Config config) {
        this.stats = stats;
        final Map<Long, IncomingMultiPartMessage> map = Collections.synchronizedMap(new LinkedHashMap());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MultiPartMessageFragment msg) throws Exception {

    }
}
