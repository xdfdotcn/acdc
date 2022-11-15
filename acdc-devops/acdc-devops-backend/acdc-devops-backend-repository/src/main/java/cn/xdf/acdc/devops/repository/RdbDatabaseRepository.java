package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data SQL repository for the RdbDatabase entity.
 */
@SuppressWarnings("unused")
@Repository
public interface RdbDatabaseRepository extends JpaRepository<RdbDatabaseDO, Long>, JpaSpecificationExecutor {

    /**
     * 根据rdb查询所有database.
     *
     * @param rdbDO rdb
     * @return java.util.List
     * @date 2022/8/10 7:07 下午
     */
    List<RdbDatabaseDO> findAllByRdb(RdbDO rdbDO);

}
