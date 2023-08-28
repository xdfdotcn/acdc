package cn.xdf.acdc.connect.starrocks.sink.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;

public final class LoadUrlValidator implements ConfigDef.Validator {
    
    @Override
    public void ensureValid(final String key, final Object value) {
        List<String> urlList = (List<String>) value;
        for (String host : urlList) {
            if (host.split(":").length < 2) {
                String errorMessage = String.format(
                        "Could not parse host '%s' in option '%s'. It should follow the format 'host_name:port'.",
                        host,
                        key);
                throw new ConfigException(key, value, errorMessage);
            }
        }
    }
}
