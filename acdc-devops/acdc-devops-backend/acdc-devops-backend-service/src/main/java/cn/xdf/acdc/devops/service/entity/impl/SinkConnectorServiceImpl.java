package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDataExtensionMappingDO;
import cn.xdf.acdc.devops.repository.SinkConnectorRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorDataExtensionService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorColumnMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorDataExtensionMappingService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class SinkConnectorServiceImpl implements SinkConnectorService {

    @Autowired
    private SinkConnectorDataExtensionMappingService sinkConnectorDataExtensionMappingService;

    @Autowired
    private ConnectorDataExtensionService connectorDataExtensionService;

    @Autowired
    private SinkConnectorColumnMappingService sinkConnectorColumnMappingService;

    @Autowired
    private SinkConnectorRepository sinkConnectorRepository;

    @Override
    public SinkConnectorDO save(final SinkConnectorDO sinkConnector) {
        return sinkConnectorRepository.save(sinkConnector);
    }

    @Override
    public SinkConnectorDO save(
            final Long kafkaTopicId,
            final Long connectorId,
            final String filterExpress) {
        return save(SinkConnectorDO.builder()
                .connector(new ConnectorDO().setId(connectorId))
                .kafkaTopic(KafkaTopicDO.builder().id(kafkaTopicId).build())
                .filterExpression(filterExpress)
                .creationTime(Instant.now())
                .updateTime(Instant.now())
                .build()
        );
    }

    @Override
    public Optional<SinkConnectorDO> findById(final Long id) {
        return sinkConnectorRepository.findById(id);
    }

    @Override
    public Optional<SinkConnectorDO> findByConnectorId(final Long id) {
        return sinkConnectorRepository.findByConnectorId(id);
    }

    @Override
    public void saveExtensionsAndColumnMappings(
            final Long sinkConnectorId,
            final List<ConnectorDataExtensionDO> dataExtensions,
            final List<SinkConnectorColumnMappingDO> columnMappings) {
        SinkConnectorDO sinkConnector = SinkConnectorDO.builder().id(sinkConnectorId).build();
        // 扩展字段, {datetime}
        List<ConnectorDataExtensionDO> savedDataExtensions = connectorDataExtensionService.saveAll(dataExtensions);
        List<SinkConnectorDataExtensionMappingDO> dataExtensionMappings = savedDataExtensions.stream().map(it ->
                SinkConnectorDataExtensionMappingDO.builder()
                        .sinkConnector(sinkConnector)
                        .connectorDataExtension(it)
                        .creationTime(Instant.now())
                        .updateTime(Instant.now())
                        .build()
        ).collect(Collectors.toList());

        sinkConnectorDataExtensionMappingService.saveAll(dataExtensionMappings);

        // 字段映射
        List<SinkConnectorColumnMappingDO> newColumnMappings = columnMappings.stream().map(it ->
                SinkConnectorColumnMappingDO.builder()
                        .sinkConnector(sinkConnector)
                        .sinkColumnName(it.getSinkColumnName())
                        .sourceColumnName(it.getSourceColumnName())
                        .creationTime(Instant.now())
                        .updateTime(Instant.now())
                        .build()
        ).collect(Collectors.toList());

        sinkConnectorColumnMappingService.saveAll(newColumnMappings);
    }
}
