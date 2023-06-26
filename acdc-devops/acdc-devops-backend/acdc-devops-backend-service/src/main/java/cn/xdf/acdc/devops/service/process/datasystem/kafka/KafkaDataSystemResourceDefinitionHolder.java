package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemResourceConfigurationDefinition.Cluster;

import java.util.HashMap;
import java.util.Map;

public final class KafkaDataSystemResourceDefinitionHolder {
    
    private static final DataSystemResourceDefinition KAFKA_DATA_SYSTEM_RESOURCE_DEFINITION = generateKafkaDataSystemResourceDefinition();
    
    private KafkaDataSystemResourceDefinitionHolder() {
    }
    
    private static DataSystemResourceDefinition generateKafkaDataSystemResourceDefinition() {
        return generateKafkaClusterDataSystemResourceDefinition();
    }
    
    private static DataSystemResourceDefinition generateKafkaClusterDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        children.put(DataSystemResourceType.KAFKA_TOPIC, generateKafkaTopicDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(generateClusterConfiguration(), DataSystemResourceType.KAFKA_CLUSTER, false, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateKafkaTopicDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(DataSystemResourceType.KAFKA_TOPIC, true, false, true);
    }

    private static Map<String, ConfigurationDefinition<?>> generateClusterConfiguration() {
        Map<String, ConfigurationDefinition<?>> configuration = new HashMap<>();

        configuration.put(Cluster.BOOTSTRAP_SERVERS.getName(), Cluster.BOOTSTRAP_SERVERS);
        configuration.put(Cluster.SASL_MECHANISM.getName(), Cluster.SASL_MECHANISM);
        configuration.put(Cluster.SECURITY_PROTOCOL_CONFIG.getName(), Cluster.SECURITY_PROTOCOL_CONFIG);
        configuration.put(Cluster.USERNAME.getName(), Cluster.USERNAME);
        configuration.put(Cluster.PASSWORD.getName(), Cluster.PASSWORD);
        
        return configuration;
    }
    
    /**
     * Get data system definition for kafka cluster.
     *
     * @return kafka cluster resource definition
     */
    public static DataSystemResourceDefinition get() {
        return KAFKA_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
