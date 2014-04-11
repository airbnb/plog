package com.airbnb.plog.handlers;

import com.typesafe.config.Config;

public interface HandlerProvider {
    public Handler getHandler(Config config) throws Exception;
}
