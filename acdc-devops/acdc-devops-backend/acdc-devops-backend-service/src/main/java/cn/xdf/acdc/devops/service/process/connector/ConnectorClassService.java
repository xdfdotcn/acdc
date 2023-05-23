package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;

import java.util.List;
import java.util.Optional;

/**
 * Connector class. TODO 与process service 进行合并
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
    
    /**
     * Get connector class detail dto by data system type and connector type.
     *
     * @param dataSystemType data system type
     * @param connectorType connector type
     * @return connector class detail dto
     */
    ConnectorClassDetailDTO getDetailByDataSystemTypeAndConnectorType(DataSystemType dataSystemType, ConnectorType connectorType);
}
