package com.airbnb.plog.console;

import com.airbnb.plog.filters.FilterProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import io.netty.channel.ChannelHandler;

import java.io.PrintStream;

public class ConsoleOutputProvider implements FilterProvider {
    @Override
    public ChannelHandler getFilter(Config config) throws Exception {
        PrintStream target = System.out;
        try {
            final String targetDescription = config.getString("target");
            if (targetDescription.toLowerCase().equals("stderr"))
                target = System.err;
        } catch (ConfigException.Missing ignored) {
        }

        return new ConsoleOutputFilter(target);
    }
}
