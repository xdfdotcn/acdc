package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import java.util.List;
import java.util.Optional;

public interface SinkConnectorService {

    /**
     * 单条保存.
     *
     * @param sinkConnector sinkConnector
     * @return SinkConnector
     */
    SinkConnectorDO save(SinkConnectorDO sinkConnector);

    /**
     * 单条保存.
     *
     * @param kafkaTopicId kafkaTopicId
     * @param connectorId connectorId
     * @param filterExpress filterExpress
     * @return SinkConnector
     */
    SinkConnectorDO save(Long kafkaTopicId, Long connectorId, String filterExpress);

    /**
     * 根据 ID 查询.
     *
     * @param id id
     * @return SinkConnector
     */
    Optional<SinkConnectorDO> findById(Long id);

    /**
     * 根据 Connector 查询.
     *
     * @param id id
     * @return SinkConnector
     */
    Optional<SinkConnectorDO> findByConnectorId(Long id);

    /**
     * 根据 保存扩展字段和字段映射.
     *
     * @param sinkConnectorId sinkConnectorId
     * @param columnMappings columnMappings
     * @param dataExtensions dataExtensions
     */
    void saveExtensionsAndColumnMappings(
        Long sinkConnectorId,
        List<ConnectorDataExtensionDO> dataExtensions,
        List<SinkConnectorColumnMappingDO> columnMappings);
}
