package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

// CHECKSTYLE:OFF

/**
 * Spring Data SQL repository for the RdbInstance entity.
 */
@SuppressWarnings("unused")
@Repository
public interface RdbInstanceRepository extends JpaRepository<RdbInstanceDO, Long>, JpaSpecificationExecutor {

    /**
     * 根据 rdb ID,host,port 查询.
     *
     * @param rdbId rdbId
     * @param host  host
     * @param port  port
     * @return RdbInstance
     */
    Optional<RdbInstanceDO> findByRdbIdAndHostAndPort(Long rdbId, String host, Integer port);

    /**
     * 根据 rdbId 和 role 查询.
     *
     * @param rdbId rdbId
     * @param role  role
     * @return RdbInstance
     */
    Optional<RdbInstanceDO> findByRdbIdAndRole(Long rdbId, RoleType role);

    /**
     * 根据rdb查询所有rdbInstance.
     *
     * @param rdbId rdb主键id
     * @return java.util.List
     * @date 2022/8/3 3:04 下午
     */
    List<RdbInstanceDO> findRdbInstanceDOSByRdbId(Long rdbId);
}
