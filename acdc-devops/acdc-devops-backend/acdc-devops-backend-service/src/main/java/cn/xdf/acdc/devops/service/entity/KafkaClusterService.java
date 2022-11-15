package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;

import java.util.List;
import java.util.Optional;

/**
 * Kafka cluster.
 */
public interface KafkaClusterService {

    /**
     * 创建 kafka 集群.
     *
     * @param kafkaCluster kafka 集群
     * @return 插入数据库成功的 kafka 集群
     */
    KafkaClusterDO save(KafkaClusterDO kafkaCluster);

    /**
     * 查询所有的 kafka集群.
     *
     * @return kafka 集群列表
     */
    List<KafkaClusterDO> findAll();

    /**
     * 根据主键查询 kafka 集群.
     *
     * @param id 主键
     * @return kafka 集群
     */
    Optional<KafkaClusterDO> findById(Long id);

    /**
     * 查询 ACDC 内部使用的kafka集群.
     *
     * @return kafka 集群
     */
    Optional<KafkaClusterDO> findInnerKafkaCluster();

    /**
     * 查询 TICDC kafka 集群.
     *
     * @return kafka 集群
     */
    Optional<KafkaClusterDO> findTicdcKafkaCluster();

}
