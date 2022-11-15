package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import java.util.List;
import java.util.Optional;

public interface SinkConnectorColumnMappingService {

    /**
     * 单条保存.
     *
     * @param sinkConnectorColumnMapping SinkColumnMapping
     * @return SinkColumnMapping
     */
    SinkConnectorColumnMappingDO save(SinkConnectorColumnMappingDO sinkConnectorColumnMapping);

    /**
     * 批量保存.
     * @param sinkConnectorColumnMappingList sinkColumnMappingList
     * @return List
     */
    List<SinkConnectorColumnMappingDO> saveAll(List<SinkConnectorColumnMappingDO> sinkConnectorColumnMappingList);

    /**
     * 查询所有.
     *
     * @return List
     */
    List<SinkConnectorColumnMappingDO> findAll();


    /**
     * 根据 SinkConnector 查询.
     * @param sinkConnectorId sinkConnectorId
     *
     * @return List
     */
    List<SinkConnectorColumnMappingDO> findBySinkConnectorId(Long sinkConnectorId);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return SinkColumnMapping
     */
    Optional<SinkConnectorColumnMappingDO> findById(Long id);

    /**
     * 根据 sink connector ID 删除.
     *
     * @param sinkConnectorId sinkConnectorId
     */
    void deleteBySinkConnectorId(Long sinkConnectorId);
}
