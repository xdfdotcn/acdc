package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;

import java.util.List;
import java.util.Optional;

/**
 * Connector default config.
 *  TODO 与process service 进行合并
 */
public interface DefaultConnectorConfigurationService {
    
    /**
     * 单条保存.
     *
     * @param defaultConnectorConfiguration DefaultConnectorConfiguration
     * @return DefaultConnectorConfiguration
     */
    DefaultConnectorConfigurationDO save(DefaultConnectorConfigurationDO defaultConnectorConfiguration);
    
    /**
     * 查询所有.
     *
     * @return List
     */
    List<DefaultConnectorConfigurationDO> findAll();
    
    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return DefaultConnectorConfiguration
     */
    Optional<DefaultConnectorConfigurationDO> findById(Long id);
    
    /**
     * 根据 Connector Class ID查询.
     *
     * @param connectorClassId connector class ID
     * @return List
     */
    List<DefaultConnectorConfigurationDO> findByConnectorClassId(Long connectorClassId);
}
