package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data SQL repository for the ConnectorClass entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorClassRepository extends JpaRepository<ConnectorClassDO, Long>,
        JpaSpecificationExecutor<ConnectorClassDO> {

    /**
     * 根据名称查询.
     *
     * @param name name
     * @return ConnectorClass
     */
    Optional<ConnectorClassDO> findOneByName(String name);

    /**
     * 根据名称查询.
     *
     * @param className      className
     * @param dataSystemType dataSystemType
     * @return ConnectorClass
     */
    Optional<ConnectorClassDO> findOneByNameAndDataSystemType(String className, DataSystemType dataSystemType);

    /**
     * find connector class by data system type and connector type.
     *
     * @param dataSystemType data system type
     * @param connectorType  connector type
     * @return connector class do
     */
    Optional<ConnectorClassDO> findOneByDataSystemTypeAndConnectorType(DataSystemType dataSystemType, ConnectorType connectorType);
}
