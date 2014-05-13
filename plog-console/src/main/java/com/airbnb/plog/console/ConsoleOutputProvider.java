package com.airbnb.plog.console;

import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.handlers.HandlerProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import java.io.PrintStream;

public final class ConsoleOutputProvider implements HandlerProvider {
    @Override
    public Handler getHandler(Config config) throws Exception {
        PrintStream target = System.out;
        try {
            final String targetDescription = config.getString("target");
            if (targetDescription.toLowerCase().equals("stderr")) {
                target = System.err;
            }
        } catch (ConfigException.Missing ignored) {
        }

        return new ConsoleOutputHandler(target);
    }
}
