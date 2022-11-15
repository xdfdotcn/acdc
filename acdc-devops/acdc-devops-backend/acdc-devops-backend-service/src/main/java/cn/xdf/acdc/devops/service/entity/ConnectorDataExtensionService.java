package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import java.util.List;
import java.util.Optional;

/**
 * Connector 扩展字段.
 */
public interface ConnectorDataExtensionService {

    /**
     * 单条保存.
     *
     * @param connectorDataExtension ConnectorDataExtension
     * @return ConnectorDataExtension
     */
    ConnectorDataExtensionDO save(ConnectorDataExtensionDO connectorDataExtension);

    /**
     * 批量保存.
     * @param connectorDataExtensionList connectorDataExtensionList
     * @return List
     */
    List<ConnectorDataExtensionDO> saveAll(List<ConnectorDataExtensionDO> connectorDataExtensionList);

    /**
     * 查询所有.
     *
     * @return List
     */
    List<ConnectorDataExtensionDO> findAll();

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return ConnectorDataExtension
     */
    Optional<ConnectorDataExtensionDO> findById(Long id);

    /**
     * 删除扩展字段.
     * @param ids ids
     */
    void deleteByIdIn(List<Long> ids);
}
