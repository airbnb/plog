package com.airbnb.plog.upshot;

import com.airbnb.plog.filters.Filter;
import com.typesafe.config.Config;

public class FilterProvider implements com.airbnb.plog.filters.FilterProvider {
    @Override
    public Filter getFilter(Config config) throws Exception {
        return new UpshotFilter();
    }
}
