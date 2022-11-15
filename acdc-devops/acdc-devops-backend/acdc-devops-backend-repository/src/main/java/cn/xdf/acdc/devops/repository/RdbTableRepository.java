package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data SQL repository for the RdbTable entity.
 */
@SuppressWarnings("unused")
@Repository
public interface RdbTableRepository extends JpaRepository<RdbTableDO, Long>, JpaSpecificationExecutor {

    /**
     * 根据rdb_database_id查询所有table.
     *
     * @param rdbDatabaseId rdb_database_id
     * @return java.util.List
     * @date 2022/8/9 4:09 下午
     */
    List<RdbTableDO> findAllByRdbDatabaseId(Long rdbDatabaseId);

}
