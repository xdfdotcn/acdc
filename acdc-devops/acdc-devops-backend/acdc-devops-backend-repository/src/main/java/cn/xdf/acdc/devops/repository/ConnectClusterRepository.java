package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data SQL repository for the ConnectCluster entity.
 */
@SuppressWarnings("unused")
@Repository
public interface ConnectClusterRepository extends JpaRepository<ConnectClusterDO, Long>, JpaSpecificationExecutor<ConnectClusterDO> {
    
    /**
     * 根据 connector class  ID 查询.
     *
     * @param connectorClassId connectorClassId
     * @return ConnectCluster
     */
    Optional<ConnectClusterDO> findOneByConnectorClassId(Long connectorClassId);
    
    /**
     * 根据 api url 查询.
     *
     * @param apiUrl apiUrl
     * @return ConnectCluster
     */
    Optional<ConnectClusterDO> findOneByConnectRestApiUrl(String apiUrl);
}
