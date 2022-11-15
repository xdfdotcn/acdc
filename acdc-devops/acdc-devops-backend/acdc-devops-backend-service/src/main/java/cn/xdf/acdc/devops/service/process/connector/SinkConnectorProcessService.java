package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkCreationDTO;

import java.util.List;

import cn.xdf.acdc.devops.service.process.datasystem.DataSystemTypeService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface SinkConnectorProcessService extends ConnectorConfigProcessService, DataSystemTypeService {

    /**
     * 创建 sink.
     *
     * @param sinkCreationDTO sinkCreationDTO
     * @return created connector
     */
    ConnectorDTO createSink(SinkCreationDTO sinkCreationDTO);

    /**
     * 编辑字段映射.
     *
     * @param connectorId connectorId
     * @param fieldMappings fieldMappings
     */
    void editFieldMapping(Long connectorId, List<FieldMappingDTO> fieldMappings);

    /**
     * 根据 source id 查询 sink.
     *
     * @param sourceConnectorId sourceId
     * @param pageable pageable
     * @return Page
     */
    Page<SinkConnectorInfoDTO> querySinkForSource(Long sourceConnectorId, Pageable pageable);

    /**
     * 获取 sink 详情信息.
     *
     * @param connectorId connectorId
     * @return SinkConnectorDetailDTO
     */
    SinkConnectorInfoDTO getSinkDetail(Long connectorId);
}
