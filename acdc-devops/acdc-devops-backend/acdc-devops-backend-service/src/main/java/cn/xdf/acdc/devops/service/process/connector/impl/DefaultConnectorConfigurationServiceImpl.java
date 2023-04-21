package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import cn.xdf.acdc.devops.repository.DefaultConnectorConfigurationRepository;
import java.util.List;
import java.util.Optional;

import cn.xdf.acdc.devops.service.process.connector.DefaultConnectorConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DefaultConnectorConfigurationServiceImpl implements DefaultConnectorConfigurationService {

    @Autowired
    private DefaultConnectorConfigurationRepository defaultConnectorConfigurationRepository;

    @Override
    public DefaultConnectorConfigurationDO save(final DefaultConnectorConfigurationDO defaultConnectorConfiguration) {
        return defaultConnectorConfigurationRepository.save(defaultConnectorConfiguration);
    }

    @Override
    public List<DefaultConnectorConfigurationDO> findAll() {
        return defaultConnectorConfigurationRepository.findAll();
    }

    @Override
    public Optional<DefaultConnectorConfigurationDO> findById(final Long id) {
        return defaultConnectorConfigurationRepository.findById(id);
    }

    @Override
    public List<DefaultConnectorConfigurationDO> findByConnectorClassId(final Long classId) {
        return defaultConnectorConfigurationRepository.findByConnectorClassId(classId);
    }
}
