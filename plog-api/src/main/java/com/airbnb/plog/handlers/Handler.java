package com.airbnb.plog.handlers;

import com.eclipsesource.json.JsonObject;
import io.netty.channel.ChannelHandler;

public interface Handler extends ChannelHandler {
    public JsonObject getStats();

    public String getName();
}
