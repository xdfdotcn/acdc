package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.ConnectorClassRepository;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Optional;

@Service
public class ConnectorClassServiceImpl implements ConnectorClassService {
    
    @Autowired
    private ConnectorClassRepository connectorClassRepository;
    
    @Override
    public ConnectorClassDO save(final ConnectorClassDO connectorClass) {
        return connectorClassRepository.save(connectorClass);
    }
    
    @Override
    public List<ConnectorClassDO> findAll() {
        return connectorClassRepository.findAll();
    }
    
    @Override
    public Optional<ConnectorClassDO> findById(final Long id) {
        return connectorClassRepository.findById(id);
    }
    
    @Override
    public Optional<ConnectorClassDO> findByClass(final String className, final DataSystemType dataSystemType) {
        return connectorClassRepository.findOneByNameAndDataSystemType(className, dataSystemType);
    }
    
    @Override
    public ConnectorClassDetailDTO getDetailByDataSystemTypeAndConnectorType(final DataSystemType dataSystemType, final ConnectorType connectorType) {
        Optional<ConnectorClassDO> optional = connectorClassRepository.findOneByDataSystemTypeAndConnectorType(dataSystemType, connectorType);
        
        if (!optional.isPresent()) {
            throw new EntityNotFoundException(String.format("can not find a connector class with data system type: %s and connector type: %s", dataSystemType.name(), connectorType.name()));
        }
        
        return new ConnectorClassDetailDTO(optional.get());
    }
}
