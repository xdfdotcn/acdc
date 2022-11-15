package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import java.util.List;
import java.util.Optional;

/**
 * Connector 集群.
 */
public interface ConnectClusterService {

    /**
     * 单条保存.
     *
     * @param connectCluster ConnectCluster
     * @return ConnectCluster
     */
    ConnectClusterDO save(ConnectClusterDO connectCluster);

    /**
     * 查询所有.
     *
     * @return List
     */
    List<ConnectClusterDO> findAll();

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return ConnectCluster
     */
    Optional<ConnectClusterDO> findById(Long id);

    /**
     * 根据 connector class 查找.
     *
     * @param connectorClassId connectorClassId
     * @return ConnectCluster
     */
    Optional<ConnectClusterDO> findByConnectorClassId(Long connectorClassId);

    /**
     * 根据集群的 rest api url 地址查询.
     *
     * @param apiUrl apiUrl
     * @return ConnectCluster
     */
    Optional<ConnectClusterDO> findByRestAPiUrl(String apiUrl);
}
