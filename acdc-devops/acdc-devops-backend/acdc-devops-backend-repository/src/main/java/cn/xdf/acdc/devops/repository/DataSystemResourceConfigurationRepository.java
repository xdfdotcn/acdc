package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceConfigurationDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the ConnectorEvent entity.
 */
@Repository
public interface DataSystemResourceConfigurationRepository extends JpaRepository<DataSystemResourceConfigurationDO, Long>,
        JpaSpecificationExecutor<DataSystemResourceConfigurationDO> {
}
