package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemResourceConfigurationDefinition.Cluster;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class KafkaDataSystemResourceDefinitionHolderTest {
    
    @Test
    void testGetShouldAsExpect() {
        DataSystemResourceDefinition kafkaDefinition = KafkaDataSystemResourceDefinitionHolder.get();
        
        // cluster
        Assertions.assertThat(kafkaDefinition.getType()).isEqualTo(DataSystemResourceType.KAFKA_CLUSTER);
        // cluster configuration
        Assertions.assertThat(kafkaDefinition.getConfigurationDefinitions()).containsValues(Cluster.BOOTSTRAP_SERVERS, Cluster.SASL_MECHANISM,
                Cluster.SECURITY_PROTOCOL_CONFIG, Cluster.USERNAME, Cluster.PASSWORD);
        
        // topic
        DataSystemResourceDefinition topicDefinition = kafkaDefinition.getChildren().get(DataSystemResourceType.KAFKA_TOPIC);
        Assertions.assertThat(topicDefinition).isNotNull();
    }
}
