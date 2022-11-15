package cn.xdf.acdc.devops.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * A cdc connector topic config.
 */
@Component
@ConfigurationProperties(prefix = "acdc.connector.topic")
@Data
public class TopicConfig {

    public static final String PARTITIONS = "partitions";

    public static final String REPLICATION_FACTOR = "replication.factor";

    private int partitionDefaultValue = 3;

    private short replicationFactorDefaultValue = 3;

    private Map<String, String> serverTopicConfig;

    private Map<String, String> schemaHistoryTopicConfig;
}
