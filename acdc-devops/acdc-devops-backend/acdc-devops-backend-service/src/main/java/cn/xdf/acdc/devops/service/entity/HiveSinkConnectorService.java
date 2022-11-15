package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HiveSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;

import java.util.Optional;

public interface HiveSinkConnectorService {

    /**
     * 单条保存.
     *
     * @param hiveSinkConnector hiveSinkConnector
     * @return HiveSinkConnector
     */
    HiveSinkConnectorDO save(HiveSinkConnectorDO hiveSinkConnector);

    /**
     * 根据 ID 查询.
     *
     * @param id id
     * @return HiveSinkConnector
     */
    Optional<HiveSinkConnectorDO> findById(Long id);


    /**
     * 根据 HiveTable 查询.
     *
     * @param id id
     * @return HiveSinkConnector
     */
    Optional<HiveSinkConnectorDO> findByHiveTableId(Long id);


    /**
     * 根据 SinkConnector 查询.
     *
     * @param id id
     * @return HiveSinkConnector
     */
    Optional<HiveSinkConnectorDO> findBySinkConnectorId(Long id);

    /**
     * 查询 sink 详情.
     *
     * @param connectorId connectorId
     * @return SinkConnectorInfo
     */
    SinkConnectorInfoDO findSinkDetail(Long connectorId);
}
