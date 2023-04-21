package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.repository.ConnectClusterRepository;
import java.util.List;
import java.util.Optional;

import cn.xdf.acdc.devops.service.process.connector.ConnectClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConnectClusterServiceImpl implements ConnectClusterService {

    @Autowired
    private ConnectClusterRepository connectClusterRepository;

    @Override
    public ConnectClusterDO save(final ConnectClusterDO connectCluster) {
        return connectClusterRepository.save(connectCluster);
    }

    @Override
    public List<ConnectClusterDO> findAll() {
        return connectClusterRepository.findAll();
    }

    @Override
    public Optional<ConnectClusterDO> findById(final Long id) {
        return connectClusterRepository.findById(id);
    }

    @Override
    public Optional<ConnectClusterDO> findByConnectorClassId(final Long connectorClassId) {
        return connectClusterRepository.findOneByConnectorClassId(connectorClassId);
    }

    @Override
    public Optional<ConnectClusterDO> findByRestAPiUrl(final String apiUrl) {
        return connectClusterRepository.findOneByConnectRestApiUrl(apiUrl);
    }
}
