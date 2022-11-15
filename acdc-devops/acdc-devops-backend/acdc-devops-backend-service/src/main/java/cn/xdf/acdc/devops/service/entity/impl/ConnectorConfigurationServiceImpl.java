package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.repository.ConnectorConfigurationRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import com.google.common.base.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class ConnectorConfigurationServiceImpl implements ConnectorConfigurationService {

    @Autowired
    private ConnectorConfigurationRepository connectorConfigurationRepository;

    @Override
    public ConnectorConfigurationDO save(final ConnectorConfigurationDO connectorConfiguration) {
        return connectorConfigurationRepository.save(connectorConfiguration);
    }

    @Override
    public List<ConnectorConfigurationDO> saveAll(final List<ConnectorConfigurationDO> connectorConfigurationList) {
        List<ConnectorConfigurationDO> configurations = connectorConfigurationRepository.saveAll(connectorConfigurationList);
        connectorConfigurationRepository.flush();
        return configurations;
    }

    @Override
    public void deleteConfigByConnectorId(final Long connectorId) {
        connectorConfigurationRepository.deleteByConnectorId(connectorId);
    }

    @Override
    public List<ConnectorConfigurationDO> saveConfig(
            final Long connectorId,
            final Map<String, String> configMap) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(configMap));

        List<ConnectorConfigurationDO> configList = configMap.entrySet().stream().map(entity ->
                ConnectorConfigurationDO.builder()
                        .name(entity.getKey())
                        .value(entity.getValue())
                        .connector(new ConnectorDO().setId(connectorId))
                        .creationTime(new Date().toInstant())
                        .updateTime(new Date().toInstant())
                        .build()
        ).collect(Collectors.toList());

        // 删除之前的配置
        deleteConfigByConnectorId(connectorId);

        return saveAll(configList);
    }

    @Override
    public List<ConnectorConfigurationDO> findByConnectorId(final Long connectorId) {
        return connectorConfigurationRepository.findByConnectorId(connectorId);
    }

    @Override
    public List<ConnectorConfigurationDO> findAll() {
        return connectorConfigurationRepository.findAll();
    }

    @Override
    public Optional<ConnectorConfigurationDO> findById(final Long id) {
        return connectorConfigurationRepository.findById(id);
    }
}
