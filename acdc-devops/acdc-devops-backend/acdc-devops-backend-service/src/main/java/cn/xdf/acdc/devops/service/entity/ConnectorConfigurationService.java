package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Connector config.
 */
public interface ConnectorConfigurationService {

    /**
     * 单条保存.
     *
     * @param connectorConfiguration ConnectorConfiguration
     * @return ConnectorConfiguration
     */
    ConnectorConfigurationDO save(ConnectorConfigurationDO connectorConfiguration);

    /**
     * 批量保存.
     * @param connectorConfigurationList connectorConfigurationList
     * @return List
     */
    List<ConnectorConfigurationDO> saveAll(List<ConnectorConfigurationDO> connectorConfigurationList);

    /**
     * 删除配置.
     * @param connectorId connectorId
     */
    void deleteConfigByConnectorId(Long connectorId);

    /**
     * 批量保存.
     * @param connectorId connectorId
     * @param configMap configMap
     * @return List
     */
    List<ConnectorConfigurationDO> saveConfig(Long connectorId, Map<String, String> configMap);

    /**
     * 查询配置.
     * @param connectorId connectorId
     * @return List
     */
    List<ConnectorConfigurationDO> findByConnectorId(Long connectorId);

    /**
     * 查询所有.
     *
     * @return List
     */
    List<ConnectorConfigurationDO> findAll();

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return ConnectorConfiguration
     */
    Optional<ConnectorConfigurationDO> findById(Long id);
}
