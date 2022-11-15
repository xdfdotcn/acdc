package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionColumnConfigurationQuery;
import cn.xdf.acdc.devops.repository.ConnectionColumnConfigurationRepository;
import cn.xdf.acdc.devops.service.entity.ConnectionColumnConfigurationService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConnectionColumnConfigurationServiceImpl implements ConnectionColumnConfigurationService {

    @Autowired
    private ConnectionColumnConfigurationRepository connectionColumnConfigurationRepository;

    @Override
    public List<ConnectionColumnConfigurationDO> query(final ConnectionColumnConfigurationQuery query) {
        return connectionColumnConfigurationRepository.findAll(ConnectionColumnConfigurationService.specificationOf(query));
    }

    @Override
    public ConnectionColumnConfigurationDO save(final ConnectionColumnConfigurationDO connectionColumnConfiguration) {
        return connectionColumnConfigurationRepository.save(connectionColumnConfiguration);
    }

    @Override
    public List<ConnectionColumnConfigurationDO> saveAll(final List<ConnectionColumnConfigurationDO> connectionColumnConfigurations) {
        return connectionColumnConfigurationRepository.saveAll(connectionColumnConfigurations);
    }

    @Override
    public Optional<ConnectionColumnConfigurationDO> findById(final Long id) {
        return connectionColumnConfigurationRepository.findById(id);
    }
}
