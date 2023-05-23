package cn.xdf.acdc.devops.service.process.connector.event;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorEventDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorEventQuery;
import org.springframework.data.domain.Page;

public interface ConnectorEventProcessService {
    
    /**
     * 查询事件,分页.
     *
     * @param query query
     * @return Page
     */
    Page<ConnectorEventDTO> queryEvent(ConnectorEventQuery query);
}
