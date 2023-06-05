package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorEventDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorEventQuery;
import cn.xdf.acdc.devops.service.process.connector.event.ConnectorEventProcessService;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1")
public class ConnectorEventController {
    
    @Autowired
    private ConnectorEventProcessService connectorEventProcessService;
    
    /**
     * 分页获取 connector 事件列表.
     *
     * @param connectorId connectorId
     * @param query query
     * @return Page
     */
    @GetMapping("/connectors/{connectorId}/events")
    public PageDTO<ConnectorEventDTO> queryConnectorEvent(
            @PathVariable("connectorId") final Long connectorId,
            final ConnectorEventQuery query) {
        if (QueryUtil.isNullId(connectorId)) {
            return PageDTO.empty();
        }
        
        query.setConnectorId(connectorId);
        Page<ConnectorEventDTO> page = connectorEventProcessService.queryEvent(query);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }
}
