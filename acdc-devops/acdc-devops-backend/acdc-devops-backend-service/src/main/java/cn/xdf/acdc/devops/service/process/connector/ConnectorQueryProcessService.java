package cn.xdf.acdc.devops.service.process.connector;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface ConnectorQueryProcessService {

    /**
     * 获取 connector 列表.
     *
     * @param query query
     * @return Page
     */
    List<ConnectorDTO> query(ConnectorQuery query);

    /**
     * 获取 connector 列表.
     *
     * @param connectorQuery connectorQuery
     * @return Page
     */
    Page<ConnectorDTO> pageQuery(ConnectorQuery connectorQuery);

    /**
     * 根据 source connector 获取对应的 sink connector.
     *
     * @param sourceConnectorId sourceConnectorId
     * @param pageable pageable
     * @param dataSystemType appType
     * @return Page
     */
    Page<SinkConnectorInfoDTO> querySinkForSource(Long sourceConnectorId, Pageable pageable, DataSystemType dataSystemType);

    /**
     * 获取 sink 详情.
     *
     * @param connectorId connectorId
     * @return Page
     */
    SinkConnectorInfoDTO getSinkInfo(Long connectorId);

    /**
     * 获取 sink 详情.
     *
     * @param connectorId connectorId
     * @return Page
     */
    SourceConnectorInfoDTO getSourceInfo(Long connectorId);

}
