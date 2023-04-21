package cn.xdf.acdc.devops.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * A cdc connector topic config.
 */
@Component
@ConfigurationProperties(prefix = "acdc.connector.topic")
@Data
public class TopicProperties {

    // data collection data topic
    private TopicConfiguration dataCollection;

    private TopicConfiguration schemaHistory;

    private TopicConfiguration schemaChange;

    private TopicConfiguration ticdc;

    @Data
    public static class TopicConfiguration {

        private int partitions;

        private short replicationFactor;

        private Acl acl;

        private Map<String, String> configs = new HashMap<>();
    }

    @Data
    public static class Acl {

        private String username;

        private String[] operations;
    }
}
