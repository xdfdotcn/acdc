package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.KafkaSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;

import java.util.Optional;

public interface KafkaSinkConnectorService {

    /**
     * 单条保存.
     *
     * @param kafkaSinkConnector kafkaSinkConnector
     * @return KafkaSinkConnector
     */
    KafkaSinkConnectorDO save(KafkaSinkConnectorDO kafkaSinkConnector);

    /**
     * 根据 ID 查询.
     *
     * @param id id
     * @return KafkaSinkConnector
     */
    Optional<KafkaSinkConnectorDO> findById(Long id);

    /**
     * 根据 KafkaTopic 查询.
     *
     * @param id id
     * @return KafkaSinkConnector
     */
    Optional<KafkaSinkConnectorDO> findByKafkaTopicId(Long id);

    /**
     * 根据 SinkConnector 查询.
     *
     * @param id id
     * @return KafkaSinkConnector
     */
    Optional<KafkaSinkConnectorDO> findBySinkConnectorId(Long id);

    /**
     * 查询详情.
     *
     * @param connectorId connectorId
     * @return SinkConnectorInfo
     */
    SinkConnectorInfoDO findSinkDetail(Long connectorId);
}
