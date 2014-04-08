package com.airbnb.plog.filters;

import com.eclipsesource.json.JsonObject;
import io.netty.channel.ChannelHandler;

public interface Filter extends ChannelHandler {
    public JsonObject getStats();

    String getName();
}
