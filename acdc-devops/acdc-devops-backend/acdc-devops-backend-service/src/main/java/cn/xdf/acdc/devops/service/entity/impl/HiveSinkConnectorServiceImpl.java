package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.HiveSinkConnectorRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.HiveSinkConnectorService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.entity.SourceRdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class HiveSinkConnectorServiceImpl implements HiveSinkConnectorService {

    @Autowired
    private HiveSinkConnectorRepository hiveSinkConnectorRepository;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private SourceRdbTableService sourceRdbTableService;

    @Override
    public HiveSinkConnectorDO save(final HiveSinkConnectorDO hiveSinkConnector) {
        return hiveSinkConnectorRepository.save(hiveSinkConnector);
    }

    @Override
    public Optional<HiveSinkConnectorDO> findById(final Long id) {
        return hiveSinkConnectorRepository.findById(id);
    }

    @Override
    public Optional<HiveSinkConnectorDO> findByHiveTableId(final Long id) {
        return hiveSinkConnectorRepository.findByHiveTableId(id);
    }

    @Override
    public Optional<HiveSinkConnectorDO> findBySinkConnectorId(final Long id) {
        return hiveSinkConnectorRepository.findBySinkConnectorId(id);
    }

    @Override
    public SinkConnectorInfoDO findSinkDetail(final Long connectorId) {
        ConnectorDO connectorDO = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        SinkConnectorDO sinkConnector = sinkConnectorService.findByConnectorId(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        HiveSinkConnectorDO hiveSinkConnector = findBySinkConnectorId(sinkConnector.getId())
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        HiveTableDO hiveTable = hiveSinkConnector.getHiveTable();

        HiveDatabaseDO hiveDatabase = hiveTable.getHiveDatabase();

        HiveDO hive = hiveDatabase.getHive();

        KafkaTopicDO kafkaTopic = sinkConnector.getKafkaTopic();

        SourceRdbTableDO sourceRdbTable = sourceRdbTableService.findByKafkaTopicId(kafkaTopic.getId())
                .orElseThrow(() -> new NotFoundException(String.format("kafkaTopic: %s", kafkaTopic)));

        RdbTableDO srcTable = sourceRdbTable.getRdbTable();
        RdbDatabaseDO srcDatabase = srcTable.getRdbDatabase();
        RdbDO srcCluster = srcDatabase.getRdb();

        DataSystemType sinkDataSystemType = DataSystemType.HIVE;
        DataSystemType srcDataSystemType = DataSystemType.nameOf(srcCluster.getRdbType());

        return SinkConnectorInfoDO.builder()
                .connectorId(connectorId)
                .name(connectorDO.getName())
                .sinkCluster(hive.getName())
                .sinkDatabase(hiveDatabase.getName())
                .sinkDataSet(hiveTable.getName())
                .sinkClusterType(DataSystemType.HIVE.getClusterType().name())
                .sinkDataSystemType(sinkDataSystemType.getName())
                .srcCluster(srcCluster.getName())
                .srcDataSystemType(srcDataSystemType.getName())
                .srcDatabase(srcDatabase.getName())
                .srcDataSet(srcTable.getName())
                .srcDataSetId(srcTable.getId())
                .build();
    }
}
