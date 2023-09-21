package cn.xdf.acdc.connect.starrocks.sink.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public final class BatchMaxSizeValidator implements ConfigDef.Validator {
    
    @Override
    public void ensureValid(final String key, final Object value) {
        Long val = (Long) value;
        if (val < 64 * StarRocksSinkConfig.MEGA_BYTES_SCALE || val > 10 * StarRocksSinkConfig.GIGA_BYTES_SCALE) {
            String errorMessage =
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [%d, %d].",
                            val, key, 64 * StarRocksSinkConfig.MEGA_BYTES_SCALE, 10 * StarRocksSinkConfig.GIGA_BYTES_SCALE);
            throw new ConfigException(key, value, errorMessage);
        }
    }
}
