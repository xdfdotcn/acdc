package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data SQL repository for the DefaultConnectorConfiguration entity.
 */

@SuppressWarnings("unused")
@Repository
public interface DefaultConnectorConfigurationRepository extends JpaRepository<DefaultConnectorConfigurationDO, Long>,
        JpaSpecificationExecutor<DefaultConnectorConfigurationDO> {
    
    /**
     * 根据 connector class ID 查询.
     *
     * @param connectorClassId connectorClassId
     * @return List
     */
    List<DefaultConnectorConfigurationDO> findByConnectorClassId(Long connectorClassId);
    
    /**
     * Find connector class by class id and class name.
     *
     * @param id id
     * @param name name
     * @return default configuration
     */
    Optional<DefaultConnectorConfigurationDO> findByConnectorClassIdAndName(Long id, String name);
}
