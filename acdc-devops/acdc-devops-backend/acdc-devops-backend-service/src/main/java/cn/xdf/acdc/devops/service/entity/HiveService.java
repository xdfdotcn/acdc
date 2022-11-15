package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.HiveDO;

import java.util.List;
import java.util.Optional;

/**
 * Hive.
 */
public interface HiveService {

    /**
     * 保存 Hive.
     *
     * @param hive Hive
     * @return 保存成功的 Hive
     */
    HiveDO save(HiveDO hive);

    /**
     * 根据ID查询 Hive.
     *
     * @param id 主键
     * @return Hive
     */
    Optional<HiveDO> findById(Long id);

    /**
     * 批量创建 Hive.
     *
     * @param hiveList 批量 Hive
     * @return 批量插入成功结果集
     */
    List<HiveDO> saveAll(List<HiveDO> hiveList);
}
