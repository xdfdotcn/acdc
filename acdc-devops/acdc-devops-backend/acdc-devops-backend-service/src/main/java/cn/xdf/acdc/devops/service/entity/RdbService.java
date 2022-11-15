package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;

import java.util.List;
import java.util.Optional;

/**
 * Rdb.
 */
public interface RdbService {

    /**
     * 创建Rdb.
     *
     * @param rdb Rdb
     * @return 插入数据库成功的 Rdb
     */
    RdbDO save(RdbDO rdb);

    /**
     * 批量创建Rdb.
     *
     * @param rdbList 批量 Rdb 集合
     * @return 批量插入成功的Rdb 集合
     */
    List<RdbDO> saveAll(List<RdbDO> rdbList);

    /**
     * 根据ID 查询 Rdb.
     *
     * @param id 主键
     * @return Rdb
     */
    Optional<RdbDO> findById(Long id);
}
