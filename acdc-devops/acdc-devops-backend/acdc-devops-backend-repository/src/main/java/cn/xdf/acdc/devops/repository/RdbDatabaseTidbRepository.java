package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseTidbDO;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RdbDatabaseTidbRepository extends JpaRepository<RdbDatabaseTidbDO, Long> {

    /**
     * 根据 rdbDatabaseId 查询.
     * @param rdbDatabaseId  rdbDatabaseId
     * @return RdbDatabaseTidb
     */
    Optional<RdbDatabaseTidbDO> findOneByRdbDatabaseId(Long rdbDatabaseId);
}
