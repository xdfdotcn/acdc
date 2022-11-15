package cn.xdf.acdc.connect.core.sink.filter;

import cn.xdf.acdc.connect.core.util.config.DestinationConfig;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class CachedFilterProvider implements FilterProvider {

    private Map<String, Filter> destinationFilterCache = new HashMap<>();

    private SinkConfig sinkConfig;

    public CachedFilterProvider(final SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    public Filter getFilter(final String destination) {
        if (!destinationFilterCache.containsKey(destination)) {
            DestinationConfig destinationConfig = sinkConfig.getDestinationConfigMapping().get(destination);
            Preconditions.checkArgument(destinationConfig != null, String.format("destination %s config empty.", destination));
            Filter filter = new ConditionsFilter(destinationConfig, sinkConfig.getTimeZone());
            destinationFilterCache.put(destination, filter);
        }
        return destinationFilterCache.get(destination);
    }

}
