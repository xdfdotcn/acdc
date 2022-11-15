package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.KafkaSinkConnectorRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.KafkaSinkConnectorService;
import cn.xdf.acdc.devops.service.entity.KafkaTopicService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.entity.SourceRdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class KafkaSinkConnectorServiceImpl implements KafkaSinkConnectorService {

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private SourceRdbTableService sourceRdbTableService;

    @Autowired
    private KafkaSinkConnectorRepository kafkaSinkConnectorRepository;

    @Autowired
    private KafkaTopicService kafkaTopicService;

    @Autowired
    private ConnectorService connectorService;

    @Override
    public KafkaSinkConnectorDO save(final KafkaSinkConnectorDO kafkaSinkConnector) {
        return kafkaSinkConnectorRepository.save(kafkaSinkConnector);
    }

    @Override
    public Optional<KafkaSinkConnectorDO> findById(final Long id) {
        return kafkaSinkConnectorRepository.findById(id);
    }

    @Override
    public Optional<KafkaSinkConnectorDO> findByKafkaTopicId(final Long id) {
        return kafkaSinkConnectorRepository.findByKafkaTopicId(id);
    }

    @Override
    public Optional<KafkaSinkConnectorDO> findBySinkConnectorId(final Long id) {
        return kafkaSinkConnectorRepository.findBySinkConnectorId(id);
    }

    @Override
    public SinkConnectorInfoDO findSinkDetail(final Long connectorId) {
        ConnectorDO connectorDO = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        SinkConnectorDO sinkConnector = sinkConnectorService.findByConnectorId(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        KafkaSinkConnectorDO kafkaSinkConnector = findBySinkConnectorId(sinkConnector.getId())
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        KafkaTopicDO kafkaTopic = kafkaSinkConnector.getKafkaTopic();
        KafkaClusterDO kafkaCluster = kafkaTopic.getKafkaCluster();

        SourceRdbTableDO sourceRdbTable = sourceRdbTableService.findByKafkaTopicId(sinkConnector.getKafkaTopic().getId())
                .orElseThrow(() -> new NotFoundException(String.format("kafkaTopic: %s", kafkaTopic)));

        RdbTableDO srcDataSet = sourceRdbTable.getRdbTable();
        RdbDatabaseDO srcDatabase = srcDataSet.getRdbDatabase();
        RdbDO srcCluster = srcDatabase.getRdb();
        DataSystemType sinkDataSystemType = DataSystemType.KAFKA;
        DataSystemType srcDataSystemType = DataSystemType.nameOf(srcCluster.getRdbType());

        return SinkConnectorInfoDO.builder()
                .connectorId(connectorId)
                .name(connectorDO.getName())
                .sinkCluster(kafkaCluster.getBootstrapServers())
                .sinkDataSet(kafkaTopic.getName())
                .sinkDataSystemType(sinkDataSystemType.getName())
                .sinkClusterType(sinkDataSystemType.getClusterType().name())
                .srcCluster(srcCluster.getName())
                .srcDataSystemType(srcDataSystemType.getName())
                .srcDatabase(srcDatabase.getName())
                .srcDataSet(srcDataSet.getName())
                .srcDataSetId(srcDataSet.getId())
                .build();
    }
}
