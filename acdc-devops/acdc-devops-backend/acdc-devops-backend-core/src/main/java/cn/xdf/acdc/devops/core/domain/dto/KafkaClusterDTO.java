package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaClusterDTO {

    private Long id;

    private Long projectId;

    private String name;

    private String version;

    private String bootstrapServers;

    private String securityConfiguration;

    private String securityProtocol;

    private String saslMechanism;

    private String saslUsername;

    private String saslPassword;

    private KafkaClusterType clusterType;

    private String description;

    public KafkaClusterDTO(final KafkaClusterDO kafkaClusterDO) {
        this.id = kafkaClusterDO.getId();
        this.name = kafkaClusterDO.getName();
        this.version = kafkaClusterDO.getVersion();
        this.bootstrapServers = kafkaClusterDO.getBootstrapServers();
        this.securityConfiguration = kafkaClusterDO.getSecurityConfiguration();
        this.clusterType = kafkaClusterDO.getClusterType();
        this.description = kafkaClusterDO.getDescription();
    }

    /**
     * Convert to kafka cluster domain object.
     *
     * @return kafka cluster domain object
     */
    public KafkaClusterDO toKafkaClusterDO() {
        return KafkaClusterDO.builder()
                .id(id)
                .name(name)
                .version(version)
                .bootstrapServers(bootstrapServers)
                .securityConfiguration(securityConfiguration)
                .clusterType(clusterType)
                .description(description)
                .build();
    }
}
