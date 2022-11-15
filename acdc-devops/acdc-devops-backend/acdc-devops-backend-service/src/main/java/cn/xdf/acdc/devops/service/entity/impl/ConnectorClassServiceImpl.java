package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.ConnectorClassRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorClassService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
}
