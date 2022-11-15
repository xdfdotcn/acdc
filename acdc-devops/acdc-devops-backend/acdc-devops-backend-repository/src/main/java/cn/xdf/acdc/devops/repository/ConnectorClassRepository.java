package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import java.util.Optional;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the ConnectorClass entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorClassRepository extends JpaRepository<ConnectorClassDO, Long> {

    /**
     * 根据名称查询.
     * @param name  name
     * @return ConnectorClass
     */
    Optional<ConnectorClassDO> findOneByName(String name);

    /**
     * 根据名称查询.
     * @param className  className
     * @param dataSystemType  dataSystemType
     * @return ConnectorClass
     */
    Optional<ConnectorClassDO> findOneByNameAndDataSystemType(String className, DataSystemType dataSystemType);
}
