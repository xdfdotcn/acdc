package cn.xdf.acdc.devops.service.process.connector.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationResultDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceCreationResultDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectorQuery;
import cn.xdf.acdc.devops.repository.ConnectorRepository;
import cn.xdf.acdc.devops.service.config.HiveJdbcConfig;
import cn.xdf.acdc.devops.service.config.TopicConfig;
import cn.xdf.acdc.devops.service.entity.ConnectClusterService;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.entity.RdbService;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.error.SystemBizException;
import cn.xdf.acdc.devops.service.process.connector.ConnectorConfigProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorTopicMangerService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Transactional
public class ConnectorCoreProcessProcessServiceImpl implements ConnectorCoreProcessService {

    @Autowired
    private HiveJdbcConfig hiveJdbcConfig;

    @Autowired
    private RdbService rdbService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private ConnectClusterService connectClusterService;

    @Autowired
    private ConnectorTopicMangerService connectorTopicMangerService;

    @Autowired
    private TopicConfig topicConfig;

    @Autowired
    private SourceConnectorProcessServiceManager sourceConnectorProcessServiceManager;

    @Autowired
    private SinkConnectorProcessServiceManager sinkConnectorProcessServiceManager;

    @Autowired
    private ConnectorRepository connectorRepository;

    private void createKafkaTopic(final String topic, final Map<String, String> configMap) {
        if (!Strings.isNullOrEmpty(topic)) {
            int partitions = Integer.parseInt(configMap.remove(TopicConfig.PARTITIONS));
            short replicationFactor = Short.parseShort(configMap.remove(TopicConfig.REPLICATION_FACTOR));
            connectorTopicMangerService.createTopicIfAbsent(topic, partitions, replicationFactor, configMap);
        }
    }

    private void createKafkaTopic(final String topic) {
        Preconditions.checkNotNull(topic);

        connectorTopicMangerService.createTopicIfAbsent(
                topic,
                topicConfig.getPartitionDefaultValue(),
                topicConfig.getReplicationFactorDefaultValue(),
                Maps.newHashMap());
    }

    private void createKafkaTopic(final SourceCreationResultDTO creationResult,
            final DataSystemType sourceDataSystemType) {
        switch (sourceDataSystemType) {
            case MYSQL:
                createKafkaTopic(creationResult.getDataTopic());
                createKafkaTopic(creationResult.getSchemaHistoryTopic(),
                        Maps.newHashMap(topicConfig.getSchemaHistoryTopicConfig()));
                createKafkaTopic(creationResult.getSourceServerTopic(),
                        Maps.newHashMap(topicConfig.getServerTopicConfig()));
                break;
            case TIDB:
                createKafkaTopic(creationResult.getDataTopic());
                break;
            default:
                throw new SystemBizException("Does not support the: " + sourceDataSystemType);
        }
    }

    @Override
    public ConnectorCreationResultDTO createSinkAndInitSource(final ConnectorCreationDTO creation) {

        DataSystemType sourceDataSystemType = creation.getSourceDataSystemType();
        DataSystemType sinkDataSystemType = creation.getSinkDataSystemType();

        // 1. create source
        SourceCreationResultDTO sourceCreationResult = sourceConnectorProcessServiceManager
                .getJService(sourceDataSystemType)
                .createSourceIfAbsent(creation.toSourceCreationDTO());

        // 2. create sink
        ConnectorDTO sourceConnector = sourceCreationResult.getCreatedConnector();
        SinkCreationDTO sinkCreation = creation.toSinkCreationDTO();
        sinkCreation.setCreatedKafkaTopicId(sourceCreationResult.getCreatedKafkaTopicId());

        ConnectorDTO sinkConnector = sinkConnectorProcessServiceManager.getJService(sinkDataSystemType)
                .createSink(sinkCreation);

        // 3. create topic ,最后创建topic,防止事务失败,尽量防止无用的topic创建, 如果最后创建topic,如果创建失败也可以导致事务回滚
        createKafkaTopic(sourceCreationResult, sourceDataSystemType);

        return ConnectorCreationResultDTO.builder()
                .sourceConnector(sourceConnector)
                .sinkConnector(sinkConnector)
                .build();
    }

    @Override
    public List<ConnectorInfoDTO> queryConnector(final Long connectClusterId, final ConnectorState currentState) {
        Preconditions.checkNotNull(connectClusterId);
        Preconditions.checkNotNull(currentState);

        ConnectClusterDO cluster = connectClusterService
                .findById(connectClusterId)
                .orElseThrow(() -> new NotFoundException(String.format("connectClusterId: %s", connectClusterId)));

        ConnectorQuery query = ConnectorQuery.builder().connectCluster(cluster).actualState(currentState).build();
        return connectorService.query(query)
                .stream().map(this::buildConnectorWithConf).collect(Collectors.toList());
    }

    @Override
    public List<ConnectorInfoDTO> queryConnector(
            final ConnectorState currentState,
            final ConnectorState desiredState,
            final Long connectClusterId) {

        Preconditions.checkNotNull(currentState);
        Preconditions.checkNotNull(desiredState);
        Preconditions.checkNotNull(connectClusterId);

        connectClusterService.findById(connectClusterId)
                .orElseThrow(() -> new NotFoundException(String.format("connectClusterId: %s", connectClusterId)));

        ConnectorQuery query = ConnectorQuery.builder()
                .connectCluster(new ConnectClusterDO(connectClusterId))
                .actualState(currentState)
                .desiredState(desiredState)
                .build();

        return connectorService.query(query)
                .stream().map(this::buildConnectorWithConf).collect(Collectors.toList());
    }

    private ConnectorInfoDTO buildConnectorWithConf(final ConnectorDO connector) {
        ConnectorType connectorType = connector.getConnectorClass().getConnectorType();
        DataSystemType dataSystemType = connector.getConnectorClass().getDataSystemType();

        ConnectorConfigProcessService connectorConfigProcessService = connectorType == ConnectorType.SOURCE
                ? sourceConnectorProcessServiceManager.getJService(dataSystemType)
                : sinkConnectorProcessServiceManager.getJService(dataSystemType);

        return new ConnectorInfoDTO(connector, connectorConfigProcessService.getDecryptConfig(connector.getId()));
    }

    @Override
    public void updateActualState(final Long connectorId, final ConnectorState state) {
        ConnectorDO connector = connectorRepository.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));
        Preconditions.checkNotNull(state);
        connector.setActualState(state);
        connector.setUpdateTime(Instant.now());
        connectorRepository.save(connector);
    }

    @Override
    public void updateDesiredState(final Long connectorId, final ConnectorState state) {
        ConnectorDO connector = connectorRepository.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));
        Preconditions.checkNotNull(state);
        connector.setDesiredState(state);
        connector.setUpdateTime(Instant.now());
        connectorRepository.save(connector);
    }

    @Override
    public void editFieldMapping(final Long connectorId, final List<FieldMappingDTO> fieldMappings) {
        ConnectorDO connector = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        DataSystemType dataSystemType = connector.getConnectorClass().getDataSystemType();

        sinkConnectorProcessServiceManager.getJService(dataSystemType).editFieldMapping(connectorId, fieldMappings);
    }

    @Override
    public Map<String, String> getEncryptConfig(final Long connectorId) {
        ConnectorDO connector = connectorService.findById(connectorId)
                .orElseThrow(() -> new NotFoundException(String.format("connectorId: %s", connectorId)));

        ConnectorType connectorType = connector.getConnectorClass().getConnectorType();
        DataSystemType dataSystemType = connector.getConnectorClass().getDataSystemType();

        ConnectorConfigProcessService connectorConfigProcessService = ConnectorType.SOURCE.equals(connectorType)
                ? sourceConnectorProcessServiceManager.getJService(dataSystemType)
                : sinkConnectorProcessServiceManager.getJService(dataSystemType);

        return connectorConfigProcessService.getEncryptConfig(connectorId);
    }
}
