package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceCreationResultDTO;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemTypeService;

public interface SourceConnectorProcessService extends ConnectorConfigProcessService, DataSystemTypeService {

    /**
     * 创建 source.
     *
     * @param sourceCreation sourceCreation
     * @return SourceCreationResultDTO
     */
    SourceCreationResultDTO createSourceIfAbsent(SourceCreationDTO sourceCreation);

    /**
     * 获取 source 详情信息.
     *
     * @param connectorId connectorId
     * @return SourceConnectorInfoDTO
     */
    SourceConnectorInfoDTO getSourceDetail(Long connectorId);

}
