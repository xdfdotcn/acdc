package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDataExtensionMappingDO;
import java.util.List;
import java.util.Optional;

public interface SinkConnectorDataExtensionMappingService {

    /**
     * 单条保存.
     *
     * @param sinkConnectorDataExtensionMapping sinkConnectorDataExtensionMapping
     * @return SinkConnectorDataExtensionMapping
     */
    SinkConnectorDataExtensionMappingDO save(SinkConnectorDataExtensionMappingDO sinkConnectorDataExtensionMapping);

    /**
     * 批量保存.
     * @param sinkConnectorDataExtensionMappingList sinkConnectorDataExtensionMappingList
     * @return List
     */
    List<SinkConnectorDataExtensionMappingDO> saveAll(List<SinkConnectorDataExtensionMappingDO> sinkConnectorDataExtensionMappingList);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return SinkColumnMapping
     */
    Optional<SinkConnectorDataExtensionMappingDO> findById(Long id);


    /**
     * 根据 SinkConnector 查询.
     * @param id 主键
     * @return List
     */
    List<SinkConnectorDataExtensionMappingDO> findBySinkConnectorId(Long id);

    /**
     * 根据 SinkConnector 删除.
     * @param sinkConnectorId sinkConnectorId
     */
    void deleteBySinkConnectorId(Long sinkConnectorId);
}
