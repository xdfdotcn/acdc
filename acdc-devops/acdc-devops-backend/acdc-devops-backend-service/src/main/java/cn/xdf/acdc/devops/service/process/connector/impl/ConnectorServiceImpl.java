package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.repository.ConnectorConfigurationRepository;
import cn.xdf.acdc.devops.repository.ConnectorRepository;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ConnectorServiceImpl implements ConnectorService {
    
    @Autowired
    private ConnectorRepository connectorRepository;
    
    @Autowired
    private ConnectorConfigurationRepository connectorConfigurationRepository;
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Override
    public ConnectorDetailDTO create(final ConnectorDetailDTO connectorDetailDTO) {
        return new ConnectorDetailDTO(connectorRepository.save(connectorDetailDTO.toDO()));
    }
    
    @Transactional
    @Override
    public Optional<ConnectorDetailDTO> getDetailByDataSystemResourceId(final Long dataSystemResourceId) {
        Optional<ConnectorDO> connectorDOOptional = connectorRepository.findByDataSystemResourceId(dataSystemResourceId);
        if (connectorDOOptional.isPresent()) {
            return Optional.of(new ConnectorDetailDTO(connectorDOOptional.get()));
        }
        return Optional.empty();
    }
    
    @Transactional
    @Override
    public Map<String, String> updateParticularConfiguration(final Long connectorId, final Map<String, String> configurations) {
        ConnectorDO connectorDO = connectorRepository.getOne(connectorId);
        
        Map<String, ConnectorConfigurationDO> nameToConfigurations = connectorDO.getConnectorConfigurations().stream()
                .collect(Collectors.toMap(each -> each.getName(), each -> each));
        configurations.forEach((name, value) -> {
            if (nameToConfigurations.containsKey(name)) {
                nameToConfigurations.get(name).setValue(value);
            } else {
                nameToConfigurations.put(name, new ConnectorConfigurationDO()
                        .setName(name)
                        .setValue(value)
                        .setConnector(connectorDO));
            }
        });
        
        connectorDO.setConnectorConfigurations(new HashSet<>(nameToConfigurations.values()));
        connectorRepository.save(connectorDO);
        return configurations;
    }
    
    @Transactional
    @Override
    public Map<String, String> updateEntireConfiguration(final Long connectorId, final Map<String, String> configurations) {
        ConnectorDO connectorDO = connectorRepository.getOne(connectorId);
        
        // delete old configurations
        Set<ConnectorConfigurationDO> oldConfigurations = connectorDO.getConnectorConfigurations();
        connectorConfigurationRepository.deleteAll(oldConfigurations);
        
        // save new configurations
        Set<ConnectorConfigurationDO> newConfigurations = configurations.entrySet().stream()
                .map(each -> {
                    ConnectorConfigurationDO configurationDO = new ConnectorConfigurationDO();
                    configurationDO.setName(each.getKey());
                    configurationDO.setValue(each.getValue());
                    configurationDO.setConnector(connectorDO);
                    return configurationDO;
                })
                .collect(Collectors.toSet());
        connectorDO.setConnectorConfigurations(newConfigurations);
        connectorRepository.save(connectorDO);
        
        return configurations;
    }
    
    @Override
    @Transactional
    public void updateActualState(final Long connectorId, final ConnectorState actualState) {
        ConnectorDO connectorDO = connectorRepository.getOne(connectorId);
        connectorDO.setActualState(actualState);
        connectorRepository.save(connectorDO);
    }
    
    @Transactional
    @Override
    public List<ConnectorDTO> query(final ConnectorQuery query) {
        return connectorRepository.query(query).stream().map(each -> new ConnectorDTO(each)).collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public List<ConnectorDetailDTO> queryDetail(final ConnectorQuery query) {
        return connectorRepository.query(query).stream().map(each -> new ConnectorDetailDTO(each)).collect(Collectors.toList());
        
    }
    
    @Override
    @Transactional
    public List<ConnectorDetailDTO> queryDetailWithDecryptConfiguration(final ConnectorQuery query) {
        List<ConnectorDetailDTO> connectorDetails = queryDetail(query);
        connectorDetails.forEach(each -> {
            DataSystemType dataSystemType = each.getDataSystemType();
            ConnectorType connectorType = each.getConnectorType();
            
            Set<String> sensitiveConfigurationNames = new HashSet<>();
            if (ConnectorType.SOURCE.equals(connectorType)) {
                sensitiveConfigurationNames.addAll(dataSystemServiceManager.getDataSystemSourceConnectorService(dataSystemType).getSensitiveConfigurationNames());
            } else {
                sensitiveConfigurationNames.addAll(dataSystemServiceManager.getDataSystemSinkConnectorService(dataSystemType).getSensitiveConfigurationNames());
            }
            
            // decrypt configuration values
            each.getConnectorConfigurations().forEach(eachConfiguration -> {
                if (sensitiveConfigurationNames.contains(eachConfiguration.getName())) {
                    eachConfiguration.setValue(EncryptUtil.decrypt(eachConfiguration.getValue()));
                }
            });
        });
        return connectorDetails;
    }
    
    @Transactional
    @Override
    public Page<ConnectorDTO> pagedQuery(final ConnectorQuery query) {
        return connectorRepository.pagedQuery(query).map(each -> new ConnectorDTO(each));
    }
    
    @Override
    @Transactional
    public void start(final Long connectorId) {
        updateDesiredState(connectorId, ConnectorState.RUNNING);
    }
    
    @Override
    @Transactional
    public void stop(final Long connectorId) {
        updateDesiredState(connectorId, ConnectorState.STOPPED);
    }
    
    private void updateDesiredState(final Long connectorId, final ConnectorState desiredState) {
        ConnectorDO connectorDO = connectorRepository.getOne(connectorId);
        connectorDO.setDesiredState(desiredState);
        connectorRepository.save(connectorDO);
    }
    
    @Override
    public ConnectorDetailDTO getDetailById(final Long connectorId) {
        return new ConnectorDetailDTO(connectorRepository.getOne(connectorId));
    }
}
