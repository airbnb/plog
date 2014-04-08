package com.airbnb.plog.filters;

import com.typesafe.config.Config;

public interface FilterProvider {
    public Filter getFilter(Config config) throws Exception;
}
