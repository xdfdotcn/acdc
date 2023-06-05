package cn.xdf.acdc.devops.service.process.kafka;

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;

import java.util.Map;
import java.util.Optional;

public interface KafkaClusterService {
    
    /**
     * Create a kafka cluster.
     *
     * @param kafkaClusterDTO kafkaClusterDTO
     * @param securityConfig security config
     * @return kafka cluster DTO
     */
    KafkaClusterDTO create(KafkaClusterDTO kafkaClusterDTO, Map<String, Object> securityConfig);
    
    /**
     * Get kafka cluster.
     *
     * @param id primary key
     * @return kafka cluster DTO
     */
    KafkaClusterDTO getById(Long id);
    
    /**
     * Get a kafka cluster by bootstrap server urls.
     *
     * @param bootstrapServers kafka bootstrap servers
     * @return optional kafka cluster DTO
     */
    Optional<KafkaClusterDTO> getByBootstrapServers(String bootstrapServers);
    
    /**
     * Get ACDC kafka cluster.
     *
     * @return ACDC kafka cluster
     */
    KafkaClusterDTO getACDCKafkaCluster();
    
    /**
     * Get ticdc kafka cluster.
     *
     * @return ACDC kafka cluster
     */
    KafkaClusterDTO getTICDCKafkaCluster();
    
    /**
     * Get kafka admin config, all sensitive configs are decrypted.
     *
     * @param id id.
     * @return kafka admin config
     */
    Map<String, Object> getDecryptedAdminConfig(Long id);
    
    /**
     * Logical delete a kafka cluster of target id.
     *
     * @param id id
     */
    void deleteById(Long id);
}
