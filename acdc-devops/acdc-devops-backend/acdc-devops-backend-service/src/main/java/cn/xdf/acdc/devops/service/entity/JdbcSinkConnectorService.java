package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.JdbcSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;

import java.util.Optional;

public interface JdbcSinkConnectorService {

    /**
     * 单条保存.
     *
     * @param jdbcSinkConnector jdbcSinkConnector
     * @return JdbcSinkConnector
     */
    JdbcSinkConnectorDO save(JdbcSinkConnectorDO jdbcSinkConnector);

    /**
     * 根据 ID 查询.
     *
     * @param id id
     * @return JdbcSinkConnector
     */
    Optional<JdbcSinkConnectorDO> findById(Long id);

    /**
     * 根据 RdbTable 查询.
     *
     * @param id id
     * @return JdbcSinkConnector
     */
    Optional<JdbcSinkConnectorDO> findByRdbTableId(Long id);

    /**
     * 根据 SinkConnector 查询.
     *
     * @param id id
     * @return JdbcSinkConnector
     */
    Optional<JdbcSinkConnectorDO> findBySinkConnectorId(Long id);

    /**
     * 查询详情.
     *
     * @param connectorId connectorId
     * @return SinkConnectorInfo
     */
    SinkConnectorInfoDO findSinkDetail(Long connectorId);
}
