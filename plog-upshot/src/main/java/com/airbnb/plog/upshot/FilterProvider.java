package com.airbnb.plog.upshot;

import com.typesafe.config.Config;
import io.netty.channel.ChannelHandler;

public class FilterProvider implements com.airbnb.plog.filters.FilterProvider {
    @Override
    public ChannelHandler getFilter(Config config) throws Exception {
        return new Filter();
    }
}
