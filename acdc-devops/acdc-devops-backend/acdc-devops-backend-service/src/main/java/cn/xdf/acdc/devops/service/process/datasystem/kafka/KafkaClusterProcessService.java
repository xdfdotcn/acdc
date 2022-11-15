package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import org.springframework.data.domain.Page;

import java.util.Map;

public interface KafkaClusterProcessService {

    /**
     * Get kafka cluster.
     *
     * @param id id
     * @return KafkaClusterDTO
     */
    KafkaClusterDTO getKafkaCluster(Long id);

    /**
     * Get kafka cluster by bootstrapServers.
     *
     * @param bootstrapServers bootstrapServers
     * @return KafkaClusterDTO
     */
    KafkaClusterDTO getKafkaClusterByBootstrapServers(String bootstrapServers);

    /**
     * Get kafka cluster and flat config.
     *
     * @param id id
     * @return KafkaClusterDTO
     */
    KafkaClusterDTO getKafkaClusterWithFlatConfig(Long id);

    /**
     * save kafka cluster and sync kafka cluster topic.
     *
     * @param dto kafka cluster DTO
     * @date 2022/9/14 10:58 上午
     */
    void saveKafkaClusterAndSyncKafkaClusterTopic(KafkaClusterDTO dto);

    /**
     * save kafka cluster.
     *
     * @param dto kafka cluster DTO
     * @return KafkaClusterDTO
     * @date 2022/9/14 10:58 上午
     */
    KafkaClusterDTO saveKafkaCluster(KafkaClusterDTO dto);

    /**
     * Create kafka cluster from internal.
     *
     * @param dto dto
     * @return KafkaClusterDTO
     */
    KafkaClusterDTO saveInternalKafkaCluster(KafkaClusterDTO dto);

    /**
     * Delete internal kafka cluster.
     *
     * @param bootstrapServers bootstrapServers
     */
    void deleteInternalKafkaCluster(String bootstrapServers);

    /**
     * update kafka cluster.
     *
     * @param dto kafka cluster DTO
     * @date 2022/9/14 10:58 上午
     */
    void updateKafkaCluster(KafkaClusterDTO dto);

    /**
     * query kafka cluster list of project by page.
     *
     * @param query query conditions
     * @return page info
     */
    Page<KafkaClusterDTO> queryByProject(KafkaClusterQuery query);


    /**
     * Get kafka admin config.
     *
     * @param id id.
     * @return kafka admin config
     */
    Map<String, Object> getAdminConfig(Long id);
}
