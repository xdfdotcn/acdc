package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring Data SQL repository for the ConnectorEvent entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorEventRepository extends JpaRepository<ConnectorEventDO, Long>, JpaSpecificationExecutor<ConnectorEventDO> {

    /**
     * Find events by connector id.
     *
     * @param connectorId connector id
     * @return connector event list
     */
    List<ConnectorEventDO> findByConnectorId(Long connectorId);
}
