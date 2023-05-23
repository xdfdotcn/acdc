package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectorEventQuery;
import cn.xdf.acdc.devops.repository.ConnectorEventRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorEventService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class ConnectorEventServiceImpl implements ConnectorEventService {
    
    private static final int MESSAGE_LENGTH_MAX = 3072;
    
    @Autowired
    private ConnectorEventRepository connectorEventRepository;
    
    @Override
    public ConnectorEventDO save(final ConnectorEventDO connectorEvent) {
        // Keep message length eq than database column max length.
        if (Objects.nonNull(connectorEvent.getMessage()) && connectorEvent.getMessage().length() > MESSAGE_LENGTH_MAX) {
            connectorEvent.setMessage(connectorEvent.getMessage().substring(0, MESSAGE_LENGTH_MAX));
        }
        
        return connectorEventRepository.save(connectorEvent);
    }
    
    @Override
    public List<ConnectorEventDO> findByConnectorId(final Long connectorId) {
        return connectorEventRepository.findByConnectorId(connectorId);
    }
    
    @Override
    public List<ConnectorEventDO> findAll() {
        return connectorEventRepository.findAll();
    }
    
    @Override
    public Page<ConnectorEventDO> query(final ConnectorEventQuery query, final Pageable pageable) {
        return connectorEventRepository.findAll(ConnectorEventService.specificationOf(query), pageable);
    }
}
