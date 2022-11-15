package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.JdbcSinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorInfoDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.repository.JdbcSinkConnectorRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.JdbcSinkConnectorService;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import cn.xdf.acdc.devops.service.entity.SinkConnectorService;
import cn.xdf.acdc.devops.service.entity.SourceRdbTableService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class JdbcSinkConnectorServiceImpl implements JdbcSinkConnectorService {

    @Autowired
    private SinkConnectorService sinkConnectorService;

    @Autowired
    private ConnectorConfigurationService connectorConfigurationService;

    @Autowired
    private RdbInstanceService rdbInstanceService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private SourceRdbTableService sourceRdbTableService;

    @Autowired
    private JdbcSinkConnectorRepository jdbcSinkConnectorRepository;

    @Override
    public JdbcSinkConnectorDO save(final JdbcSinkConnectorDO jdbcSinkConnector) {
        return jdbcSinkConnectorRepository.save(jdbcSinkConnector);
    }

    @Override
    public Optional<JdbcSinkConnectorDO> findById(final Long id) {
        return jdbcSinkConnectorRepository.findById(id);
    }

    @Override
    public Optional<JdbcSinkConnectorDO> findByRdbTableId(final Long id) {
        return jdbcSinkConnectorRepository.findByRdbTableId(id);
    }

    @Override
    public Optional<JdbcSinkConnectorDO> findBySinkConnectorId(final Long id) {
        return jdbcSinkConnectorRepository.findBySinkConnectorId(id);
    }

    @Override
    public SinkConnectorInfoDO findSinkDetail(final Long connectorId) {
        ConnectorDO connectorDO = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        SinkConnectorDO sinkConnector = sinkConnectorService.findByConnectorId(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        JdbcSinkConnectorDO jdbcSinkConnector = findBySinkConnectorId(sinkConnector.getId())
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        RdbTableDO rdbTable = jdbcSinkConnector.getRdbTable();

        RdbDatabaseDO rdbDatabase = rdbTable.getRdbDatabase();

        RdbDO rdb = rdbDatabase.getRdb();

        KafkaTopicDO kafkaTopic = sinkConnector.getKafkaTopic();

        SourceRdbTableDO sourceRdbTable = sourceRdbTableService.findByKafkaTopicId(kafkaTopic.getId())
                .orElseThrow(() -> new NotFoundException(String.format("kafkaTopic: %s", kafkaTopic)));

        RdbTableDO srcTable = sourceRdbTable.getRdbTable();
        RdbDatabaseDO srcDatabase = srcTable.getRdbDatabase();
        RdbDO srcCluster = srcDatabase.getRdb();

        DataSystemType sinkDataSystemType = DataSystemType.nameOf(rdb.getRdbType());
        DataSystemType srcDataSystemType = DataSystemType.nameOf(srcCluster.getRdbType());

        return SinkConnectorInfoDO.builder()
                .connectorId(connectorId)
                .name(connectorDO.getName())
                .sinkCluster(rdb.getName())
                .sinkDatabase(rdbDatabase.getName())
                .sinkDataSet(rdbTable.getName())
                .sinkDataSystemType(sinkDataSystemType.getName())
                .sinkClusterType(sinkDataSystemType.getClusterType().name())
                .srcCluster(srcCluster.getName())
                .srcDataSystemType(srcDataSystemType.getName())
                .srcDatabase(srcDatabase.getName())
                .srcDataSet(srcTable.getName())
                .srcDataSetId(srcTable.getId())
                .build();
    }
}
