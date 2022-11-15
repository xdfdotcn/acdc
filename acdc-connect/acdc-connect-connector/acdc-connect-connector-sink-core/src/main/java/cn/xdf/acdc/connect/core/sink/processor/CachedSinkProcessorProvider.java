package cn.xdf.acdc.connect.core.sink.processor;

import cn.xdf.acdc.connect.core.util.config.DestinationConfig;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class CachedSinkProcessorProvider implements ProcessorProvider<SinkProcessor> {

    private Map<String, SinkProcessor> processorCache = new HashMap<>();

    private SinkConfig sinkConfig;

    public CachedSinkProcessorProvider(final SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    public SinkProcessor getProcessor(final String destination) {
        Preconditions.checkArgument(sinkConfig.getDestinations().contains(destination), "Configuration for destination {} not existed, please check your connector configuration", destination);

        return processorCache.computeIfAbsent(destination, key -> {
            DestinationConfig destinationConfig = sinkConfig.getDestinationConfigMapping().get(key);
            return new SinkProcessor(destinationConfig, sinkConfig.getTimeZone());
        });
    }

}
