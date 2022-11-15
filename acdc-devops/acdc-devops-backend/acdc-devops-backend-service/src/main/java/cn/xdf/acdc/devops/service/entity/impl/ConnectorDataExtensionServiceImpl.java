package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.repository.ConnectorDataExtensionRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorDataExtensionService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConnectorDataExtensionServiceImpl implements ConnectorDataExtensionService {

    @Autowired
    private ConnectorDataExtensionRepository connectorDataExtensionRepository;

    @Override
    public ConnectorDataExtensionDO save(final ConnectorDataExtensionDO connectorDataExtension) {
        return connectorDataExtensionRepository.save(connectorDataExtension);
    }

    @Override
    public List<ConnectorDataExtensionDO> saveAll(final List<ConnectorDataExtensionDO> connectorDataExtensionList) {
        List<ConnectorDataExtensionDO> saveResult = connectorDataExtensionRepository.saveAll(connectorDataExtensionList);
//        connectorDataExtensionRepository.flush();
        return saveResult;
    }

    @Override
    public List<ConnectorDataExtensionDO> findAll() {
        return connectorDataExtensionRepository.findAll();
    }

    @Override
    public Optional<ConnectorDataExtensionDO> findById(final Long id) {
        return connectorDataExtensionRepository.findById(id);
    }

    @Override
    public void deleteByIdIn(final List<Long> ids) {
        connectorDataExtensionRepository.deleteByIdIn(ids);
    }
}
