package com.airbnb.plog.filters;

import com.typesafe.config.Config;
import io.netty.channel.ChannelHandler;

public abstract class FilterProvider {
    public abstract ChannelHandler getFilter(Config config) throws Exception;
}
