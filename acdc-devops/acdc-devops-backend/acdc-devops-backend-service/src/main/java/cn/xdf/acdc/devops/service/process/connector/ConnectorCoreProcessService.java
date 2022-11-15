package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationResultDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;

import java.util.List;
import java.util.Map;

/**
 * Connector 核心操作接口 .
 */

public interface ConnectorCoreProcessService {

    /**
     * 创建链路.
     *
     * @param creation 链路创建信息
     * @return ConnectorCreationResultDTO
     */
    ConnectorCreationResultDTO createSinkAndInitSource(ConnectorCreationDTO creation);

    /**
     * 查询 connector 列表.
     *
     * @param connectClusterId connectClusterId
     * @param currentState connector 当前状态
     * @return connector 列表
     */
    List<ConnectorInfoDTO> queryConnector(Long connectClusterId, ConnectorState currentState);

    /**
     * 查询 connector 列表.
     *
     * @param currentState 当前状态
     * @param desiredState 期望状态
     * @param connectClusterId connectClusterId
     * @return connector 列表
     */
    List<ConnectorInfoDTO> queryConnector(ConnectorState currentState, ConnectorState desiredState, Long connectClusterId);

    /**
     * 更新 connector 实际状态.
     *
     * @param connectorId connector ID
     * @param state connector 状态值
     */
    void updateActualState(Long connectorId, ConnectorState state);

    /**
     * 更新 connector 预期状态.
     *
     * @param connectorId connector ID
     * @param state connector 状态值
     */
    void updateDesiredState(Long connectorId, ConnectorState state);

    /**
     * 更新字段映射.
     *
     * @param connectorId connectorId
     * @param fieldMappings fieldMappings
     */
    void editFieldMapping(Long connectorId, List<FieldMappingDTO> fieldMappings);


    /**
     * 获取加密的配置.
     *
     * @param connectorId connectorId
     * @return 配置信息
     */
    Map<String, String> getEncryptConfig(Long connectorId);
}
