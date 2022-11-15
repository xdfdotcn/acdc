package cn.xdf.acdc.devops.service.process.datasystem.rdb;

import cn.xdf.acdc.devops.core.domain.dto.MysqlDataSourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;

import java.util.List;

public interface RdbProcessService {

    /**
     * 保存 rdb mysql 实例信息.
     *
     * @param mysqlDataSourceDTO mysqlDataSourceDTO
     */

    void saveMysqlInstance(MysqlDataSourceDTO mysqlDataSourceDTO);


    /**
     * 获取实例信息.
     *
     * @param rdbId rdbId
     * @return RdbMysqlDTO
     */
    MysqlDataSourceDTO getRdbMysqlInstance(Long rdbId);

    /**
     * Get rdb.
     *
     * @param id id
     * @return Rdb
     */
    RdbDTO getRdb(Long id);

    /**
     * 创建rdb.
     *
     * @param rdbDTO rdb信息
     */
    void saveRdb(RdbDTO rdbDTO);

    /**
     * 修改rdb.
     *
     * @param rdbDTO rdb信息
     */
    void updateRdb(RdbDTO rdbDTO);

    /**
     * 增量更新rdb和db实例和project.
     *
     * @param rdbList rdbList
     */
    void incrementalUpdateRdbsWithRelatedDbInstanceAndProject(List<RdbDO> rdbList);
}
