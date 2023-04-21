package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api")
@Transactional
public class ConnectorController {

    @Autowired
    private ConnectorService connectorService;

    /**
     * Start the connector.
     *
     * @param connectorId connectorId
     */
    @PostMapping("/connectors/{connectorId}/start")
    public void start(@PathVariable("connectorId") final Long connectorId) {
        connectorService.start(connectorId);
    }

    /**
     * Stop the connector.
     *
     * @param connectorId connectorId
     */
    @PostMapping("/connectors/{connectorId}/stop")
    public void stop(@PathVariable("connectorId") final Long connectorId) {
        connectorService.stop(connectorId);
    }

    /**
     * 查询 connector 列表.
     *
     * @param connectorQuery connectorQuery
     * @return Page
     */
    @GetMapping("/connectors")
    public PageDTO<ConnectorDTO> queryConnector(final ConnectorQuery connectorQuery) {
        Page page = connectorService.pagedQuery(connectorQuery);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }

    /**
     * Get connector by id.
     *
     * @param connectorId connector id
     * @return connector detail DTO
     */
    @GetMapping("/connectors/{connectorId}")
    public ConnectorDetailDTO getById(@PathVariable("connectorId") final Long connectorId) {
        return connectorService.getDetailById(connectorId);
    }
}
