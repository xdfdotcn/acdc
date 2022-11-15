package cn.xdf.acdc.devops.service.process.datasystem.rdb;

import cn.xdf.acdc.devops.core.domain.dto.RdbInstanceDTO;

import java.util.List;

public interface RdbInstanceProcessService {

    /**
     * Get rdb instance.
     *
     * @param id id
     * @return RdbInstanceDTO
     */
    RdbInstanceDTO getRdbInstance(Long id);

    /**
     * 根据rdb查询实例列表.
     *
     * @param id rdb主键
     * @return java.util.List
     * @date 2022/8/3 2:57 下午
     */
    List<RdbInstanceDTO> queryInstancesByRdbId(Long id);

    /**
     * 批量添加rdb集群实例.
     *
     * @param rdbId              rdb id
     * @param rdbInstanceDTOList instance列表
     * @date 2022/8/3 4:12 下午
     */
    void saveRdbInstances(Long rdbId, List<RdbInstanceDTO> rdbInstanceDTOList);


    /**
     * Get data source instance by rdbId.
     *
     * @param rdbId rdbId
     * @return RdbInstanceDTO
     */
    RdbInstanceDTO getDataSourceInstanceByRdbId(Long rdbId);
}
