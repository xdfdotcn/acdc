package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
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
    public KafkaClusterDO toDO() {
        return new KafkaClusterDO()
                .setId(id)
                .setName(name)
                .setVersion(version)
                .setBootstrapServers(bootstrapServers)
                .setSecurityConfiguration(securityConfiguration)
                .setClusterType(clusterType)
                .setDescription(description);
    }
}
