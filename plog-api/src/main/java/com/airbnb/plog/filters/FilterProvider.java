package com.airbnb.plog.filters;

import com.typesafe.config.Config;
import io.netty.channel.ChannelHandler;

public interface FilterProvider {
    public ChannelHandler getFilter(Config config) throws Exception;
}
