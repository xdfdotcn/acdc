package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationValueType;
import com.google.common.collect.Sets;

import java.util.Set;

public class KafkaSinkConnectorSpecificConfigurationDefinition {

    public static class Sink {

        private static final String DATA_FORMAT_TYPE_KEY = "dataFormatType";

        private static final String DATA_FORMAT_TYPE_DESC = "数据格式";

        public static final ConfigurationDefinition<DataFormatType> DATA_FORMAT_TYPE = new ConfigurationDefinition(
                false,
                false,
                DATA_FORMAT_TYPE_KEY,
                DATA_FORMAT_TYPE_DESC,
                DataFormatType.SCHEMA_LESS_JSON,
                ConfigurationValueType.ENUM,
                ConfigurationValueType.values(), value -> true
        );

        public static final Set<ConfigurationDefinition> SPECIFIC_CONFIGURATION_DEFINITIONS = Sets.newHashSet(DATA_FORMAT_TYPE);
    }
}
