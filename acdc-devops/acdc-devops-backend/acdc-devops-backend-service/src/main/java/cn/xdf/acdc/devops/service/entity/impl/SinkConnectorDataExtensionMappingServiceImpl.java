package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDataExtensionMappingDO;
import cn.xdf.acdc.devops.repository.SinkConnectorDataExtensionMappingRepository;
import cn.xdf.acdc.devops.service.entity.SinkConnectorDataExtensionMappingService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SinkConnectorDataExtensionMappingServiceImpl implements SinkConnectorDataExtensionMappingService {

    @Autowired
    private SinkConnectorDataExtensionMappingRepository sinkConnectorDataExtensionMappingRepository;

    @Override
    public SinkConnectorDataExtensionMappingDO save(
        final SinkConnectorDataExtensionMappingDO sinkConnectorDataExtensionMapping) {
        return sinkConnectorDataExtensionMappingRepository.save(sinkConnectorDataExtensionMapping);
    }

    @Override
    public List<SinkConnectorDataExtensionMappingDO> saveAll(
        final List<SinkConnectorDataExtensionMappingDO> sinkConnectorDataExtensionMappingList) {
        List<SinkConnectorDataExtensionMappingDO> saveResult =
            sinkConnectorDataExtensionMappingRepository.saveAll(sinkConnectorDataExtensionMappingList);
//        sinkConnectorDataExtensionMappingRepository.flush();
        return saveResult;
    }

    @Override
    public Optional<SinkConnectorDataExtensionMappingDO> findById(final Long id) {
        return sinkConnectorDataExtensionMappingRepository.findById(id);
    }

    @Override
    public List<SinkConnectorDataExtensionMappingDO> findBySinkConnectorId(final Long id) {
        return sinkConnectorDataExtensionMappingRepository.findBySinkConnectorId(id);
    }

    @Override
    public void deleteBySinkConnectorId(final Long sinkConnectorId) {
        sinkConnectorDataExtensionMappingRepository.deleteBySinkConnectorId(sinkConnectorId);
    }
}
