package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseTidbDO;
import java.util.List;
import java.util.Optional;

public interface RdbDatabaseTidbService {

    /**
     * 单条保存.
     *
     * @param rdbDatabaseTidb rdbDatabaseTidb
     * @return RdbDatabaseTidb
     */
    RdbDatabaseTidbDO save(RdbDatabaseTidbDO rdbDatabaseTidb);

    /**
     * 批量创建.
     *
     * @param rdbDatabaseTidbList rdbDatabaseTidbList
     * @return list
     */
    List<RdbDatabaseTidbDO> saveAll(List<RdbDatabaseTidbDO> rdbDatabaseTidbList);

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return RdbTidb
     */
    Optional<RdbDatabaseTidbDO> findById(Long id);

    /**
     * 根据 rdbDatabaseId 查询.
     *
     * @param rdbDatabaseId rdbDatabaseId
     * @return 分页列表
     */
    Optional<RdbDatabaseTidbDO> findByRdbDataBaseId(Long rdbDatabaseId);

    /**
     * 查询所有数据.
     *
     * @return List
     */
    List<RdbDatabaseTidbDO> findAll();

}
