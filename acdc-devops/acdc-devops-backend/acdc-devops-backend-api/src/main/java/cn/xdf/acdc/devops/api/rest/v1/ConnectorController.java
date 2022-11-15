package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorQueryProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("api")
@Transactional
public class ConnectorController {

    @Autowired
    private ConnectorQueryProcessService connectorQueryProcessService;

    @Autowired
    private ConnectorCoreProcessService connectorCoreProcessService;

    /**
     * 修改 connector 状态.
     *
     * @param connectorId connectorId
     * @param state       state
     */
    @PutMapping("/connectors/{connectorId}/status")
    public void editConnectorStatus(
            @PathVariable("connectorId") final Long connectorId,
            final ConnectorState state) {
        connectorCoreProcessService.updateDesiredState(connectorId, state);
    }

    /**
     * 获取 connector 配置.
     *
     * @param connectorId connectorId
     * @return Map
     */
    @GetMapping("/connectors/{connectorId}/config")
    public Map<String, String> getConfig(
            @PathVariable("connectorId") final Long connectorId
    ) {
        return connectorCoreProcessService.getEncryptConfig(connectorId);
    }

    /**
     * 查询 connector 列表.
     *
     * @param connectorQuery connectorQuery
     * @return Page
     */
    @GetMapping("/connectors")
    public PageDTO<ConnectorDTO> queryConnector(final ConnectorQuery connectorQuery) {
        Page page = connectorQueryProcessService.pageQuery(connectorQuery);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }

    /**
     * 获取 sink 列表.
     *
     * @param sinkConnector sinkConnector
     * @return Page
     */
    @GetMapping("/connectors/sinks")
    public PageDTO<SinkConnectorInfoDTO> querySinks(
            final SinkConnectorInfoDTO sinkConnector
    ) {
        Long connectorId = sinkConnector.getSourceConnectorId();
        DataSystemType sinkDataSystemType = DataSystemType.nameOf(sinkConnector.getSinkDataSystemType());
        Pageable pageable = PagedQuery.ofPage(sinkConnector.getCurrent(), sinkConnector.getPageSize(), SinkConnectorInfoDTO.SORT_FIELD);
        Page result = connectorQueryProcessService.querySinkForSource(connectorId, pageable, sinkDataSystemType);

        return PageDTO.of(result.getContent(), result.getTotalElements());
    }

    /**
     * 获取 sink 详情.
     *
     * @param connectorId connectorId
     * @return Page
     */
    @GetMapping("/connectors/sinks/{connectorId}")
    public SinkConnectorInfoDTO getSinkInfo(
            @PathVariable("connectorId") final Long connectorId
    ) {
        return connectorQueryProcessService.getSinkInfo(connectorId);
    }

    /**
     * 获取 source 详情.
     *
     * @param connectorId connectorId
     * @return Page
     */
    @GetMapping("/connectors/sources/{connectorId}")
    public SourceConnectorInfoDTO getSourceInfo(
            @PathVariable("connectorId") final Long connectorId
    ) {
        return connectorQueryProcessService.getSourceInfo(connectorId);
    }
}
