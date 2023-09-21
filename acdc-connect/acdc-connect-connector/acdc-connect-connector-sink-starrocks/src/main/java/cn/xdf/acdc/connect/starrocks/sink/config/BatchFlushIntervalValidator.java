package cn.xdf.acdc.connect.starrocks.sink.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public final class BatchFlushIntervalValidator implements ConfigDef.Validator {
    
    @Override
    public void ensureValid(final String key, final Object value) {
        Long val = (Long) value;
        
        if (val < 1000L || val > 3600000L) {
            String errorMessage =
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [1000, 3600000].",
                            val, key);
            
            throw new ConfigException(key, value, errorMessage);
        }
    }
}
