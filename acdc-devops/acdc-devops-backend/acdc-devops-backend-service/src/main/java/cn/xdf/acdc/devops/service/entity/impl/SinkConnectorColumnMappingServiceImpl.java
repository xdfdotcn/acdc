package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.repository.SinkConnectorColumnMappingRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorDataExtensionService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorColumnMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorDataExtensionMappingService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SinkConnectorColumnMappingServiceImpl implements SinkConnectorColumnMappingService {

    @Autowired
    private SinkConnectorColumnMappingRepository sinkConnectorColumnMappingRepository;

    @Autowired
    private SinkConnectorDataExtensionMappingService sinkConnectorDataExtensionMappingService;

    @Autowired
    private ConnectorDataExtensionService connectorDataExtensionService;

    @Override
    public SinkConnectorColumnMappingDO save(final SinkConnectorColumnMappingDO sinkConnectorColumnMapping) {
        return sinkConnectorColumnMappingRepository.save(sinkConnectorColumnMapping);
    }

    @Override
    public List<SinkConnectorColumnMappingDO> saveAll(
        final List<SinkConnectorColumnMappingDO> sinkConnectorColumnMappingList) {
        List<SinkConnectorColumnMappingDO> saveResult =
            sinkConnectorColumnMappingRepository.saveAll(sinkConnectorColumnMappingList);
//        sinkConnectorColumnMappingRepository.flush();
        return saveResult;
    }

    @Override
    public List<SinkConnectorColumnMappingDO> findAll() {
        return sinkConnectorColumnMappingRepository.findAll();
    }

    @Override
    public List<SinkConnectorColumnMappingDO> findBySinkConnectorId(final Long sinkConnectorId) {
        return sinkConnectorColumnMappingRepository.findBySinkConnectorId(sinkConnectorId);
    }

    @Override
    public Optional<SinkConnectorColumnMappingDO> findById(final Long id) {
        return sinkConnectorColumnMappingRepository.findById(id);
    }

    @Override
    public void deleteBySinkConnectorId(final Long sinkConnectorId) {
        sinkConnectorColumnMappingRepository.deleteBySinkConnectorId(sinkConnectorId);
    }
}
