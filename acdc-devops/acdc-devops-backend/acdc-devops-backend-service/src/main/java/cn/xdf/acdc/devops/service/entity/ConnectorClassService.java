package cn.xdf.acdc.devops.service.entity;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

import java.util.List;
import java.util.Optional;

/**
 * Connector class.
 */
public interface ConnectorClassService {

    /**
     * 单条保存.
     *
     * @param connectorClass ConnectorClass
     * @return ConnectorClass
     */
    ConnectorClassDO save(ConnectorClassDO connectorClass);

    /**
     * 查询所有.
     *
     * @return List
     */
    List<ConnectorClassDO> findAll();

    /**
     * 根据ID查询.
     *
     * @param id 主键
     * @return ConnectorClass
     */
    Optional<ConnectorClassDO> findById(Long id);

    /**
     * 根据class 名称查询.
     *
     * @param className class name
     * @param dataSystemType dataSystemType
     * @return ConnectorClass
     */
    Optional<ConnectorClassDO> findByClass(String className, DataSystemType dataSystemType);
}
