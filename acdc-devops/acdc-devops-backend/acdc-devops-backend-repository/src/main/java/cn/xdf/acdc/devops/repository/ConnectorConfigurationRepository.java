package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data SQL repository for the ConnectorConfiguration entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectorConfigurationRepository extends JpaRepository<ConnectorConfigurationDO, Long> {

    /**
     * 根据 connector ID 删除.
     * @param connectorId   connectorId
     */
    void deleteByConnectorId(Long connectorId);

    /**
     * 根据 connector ID 查找.
     * @param connectorId   connectorId
     * @return List
     */
    List<ConnectorConfigurationDO> findByConnectorId(Long connectorId);

}
