package cn.xdf.acdc.devops.service.process.connector.event.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorEventDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorEventQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.service.entity.ConnectorEventService;
import cn.xdf.acdc.devops.service.process.connector.event.ConnectorEventProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class ConnectorEventProcessServiceImpl implements ConnectorEventProcessService {
    
    @Autowired
    private ConnectorEventService connectorEventService;
    
    @Override
    public Page<ConnectorEventDTO> queryEvent(final ConnectorEventQuery query) {
        Pageable pageable = PagedQuery.pageOf(query.getCurrent(), query.getPageSize(), ConnectorEventQuery.SORT_FIELD);
        return connectorEventService.query(query, pageable).map(ConnectorEventDTO::new);
    }
}
