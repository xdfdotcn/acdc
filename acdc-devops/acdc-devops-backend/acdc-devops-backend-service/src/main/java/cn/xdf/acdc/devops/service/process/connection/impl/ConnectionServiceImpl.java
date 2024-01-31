package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.repository.ConnectionColumnConfigurationRepository;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
import cn.xdf.acdc.devops.service.process.project.ProjectService;
import cn.xdf.acdc.devops.service.process.user.UserService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Connection;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import joptsimple.internal.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ConnectionServiceImpl implements ConnectionService {
    
    @Autowired
    private ConnectionRepository connectionRepository;
    
    @Autowired
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private ConnectorService connectorService;
    
    @Autowired
    private UserService userService;
    
    @Autowired
    private I18nService i18n;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private ProjectService projectService;
    
    @Autowired
    private KafkaClusterService kafkaClusterService;
    
    @Autowired
    private KafkaTopicService kafkaTopicService;
    
    @Autowired
    private ConnectionColumnConfigurationRepository connectionColumnConfigurationRepository;
    
    @PersistenceContext
    private EntityManager entityManager;
    
    @Transactional
    @Override
    public List<ConnectionDTO> query(final ConnectionQuery query) {
        return connectionRepository.query(query).stream().map(ConnectionDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public ConnectionDTO getById(final Long id) {
        return new ConnectionDTO(connectionRepository.getOne(id));
    }
    
    @Transactional
    @Override
    public List<ConnectionDTO> getByIds(final Set<Long> connectionIds) {
        return connectionRepository.findAllById(connectionIds)
                .stream()
                .map(ConnectionDTO::new)
                .collect(Collectors.toList());
    }
    
    @Transactional
    @Override
    public ConnectionDetailDTO getDetailById(final Long id) {
        return new ConnectionDetailDTO(connectionRepository.getOne(id));
    }
    
    @Transactional
    @Override
    public void deleteById(final Long id) {
        ConnectionDO connectionDO = connectionRepository.getOne(id);
        connectionDO.setDeleted(Boolean.TRUE);
        connectionDO.setDesiredState(ConnectionState.STOPPED);
        connectionRepository.save(connectionDO);
    }
    
    @Transactional
    @Override
    public void deleteByIds(final Set<Long> ids) {
        ids.forEach(this::deleteById);
    }
    
    @Transactional
    @Override
    public Page<ConnectionDTO> pagedQuery(final ConnectionQuery query) {
        return connectionRepository.pagedQuery(query).map(ConnectionDTO::new);
    }
    
    @Transactional
    @Override
    public List<ConnectionDetailDTO> batchCreate(final List<ConnectionDetailDTO> connections, final String domainAccount) {
        // 1. check params
        checkParameterForCreateConnections(connections);
        // 2. check connection meta data
        checkMetaForCreateConnections(connections);
        
        // 2. cascade save connection and connection configurations
        UserDTO userDTO = userService.getByDomainAccount(domainAccount);
        
        List<ConnectionDetailDTO> savedConnectionDetailDTOs = Lists.newArrayListWithCapacity(connections.size());
        for (ConnectionDetailDTO detail : connections) {
            detail.setUserId(userDTO.getId());
            ConnectionDO savedConnectionDO = connectionRepository.save(detail.toDO());
            entityManager.refresh(savedConnectionDO);
            savedConnectionDetailDTOs.add(new ConnectionDetailDTO(savedConnectionDO));
        }
        
        return savedConnectionDetailDTOs;
    }
    
    private void checkMetaForCreateConnections(final List<ConnectionDetailDTO> connections) {
        Set<String> connectionUniqueKeySet = Sets.newHashSet();
        Set<Long> sourceDataCollectionIds = Sets.newHashSet();
        Set<Long> sinkDataCollectionIds = Sets.newHashSet();
        List<Long> sourceProjectIds = Lists.newArrayList();
        List<Long> sinkProjectIds = Lists.newArrayList();
        Set<Long> sinkInstanceIds = Sets.newHashSet();
        
        for (ConnectionDetailDTO detail : connections) {
            sourceDataCollectionIds.add(detail.getSourceDataCollectionId());
            sinkDataCollectionIds.add(detail.getSinkDataCollectionId());
            sourceProjectIds.add(detail.getSourceProjectId());
            sinkProjectIds.add(detail.getSinkProjectId());
            if (Objects.nonNull(detail.getSinkInstanceId())) {
                sinkInstanceIds.add(detail.getSinkInstanceId());
            }
        }
        
        Map<Long, ProjectDTO> sourceProjectMapping = getProjectMapping(sourceProjectIds);
        Map<Long, ProjectDTO> sinkProjectMapping = getProjectMapping(sinkProjectIds);
        Map<Long, DataSystemResourceDTO> sourceDataCollectionMapping = getDataSystemResourceMapping(sourceDataCollectionIds);
        Map<Long, DataSystemResourceDTO> sinkDataCollectionMapping = getDataSystemResourceMapping(sinkDataCollectionIds);
        Map<Long, DataSystemResourceDTO> sinkInstanceMapping = getDataSystemResourceMapping(sinkInstanceIds);
        Map<String, ConnectionDTO> alreadyExistConnectionMapping = getAlreadyExistConnectionMapping(sinkDataCollectionIds);
        
        for (ConnectionDetailDTO detail : connections) {
            // check project existence
            if (!sourceProjectMapping.containsKey(detail.getSourceProjectId())) {
                throw new EntityNotFoundException(i18n.msg(Connection.SOURCE_PROJECT_NOT_FOUND, detail.getSourceProjectId()));
            }
            if (!sinkProjectMapping.containsKey(detail.getSinkProjectId())) {
                throw new EntityNotFoundException(i18n.msg(Connection.SINK_PROJECT_NOT_FOUND, detail.getSinkProjectId()));
            }
            
            // check source project owner
            String sourceOwnerName = sourceProjectMapping.get(detail.getSourceProjectId()).getOwnerName();
            if (Strings.isNullOrEmpty(sourceOwnerName)) {
                throw new EntityNotFoundException(i18n.msg(Connection.SOURCE_PROJECT_OWNER_NOT_FOUND, detail.getSourceProjectId()));
            }
            
            // check source data collection existence
            if (!sourceDataCollectionMapping.containsKey(detail.getSourceDataCollectionId())) {
                throw new EntityNotFoundException(i18n.msg(Connection.SOURCE_DATA_COLLECTION_NOT_FOUND, detail.getSourceDataCollectionId()));
            }
            if (!sinkDataCollectionMapping.containsKey(detail.getSinkDataCollectionId())) {
                throw new EntityNotFoundException(i18n.msg(Connection.SINK_DATA_COLLECTION_NOT_FOUND, detail.getSinkDataCollectionId()));
            }
            
            // check sink instance
            if (Objects.nonNull(detail.getSinkInstanceId())
                    && !sinkInstanceMapping.containsKey(detail.getSinkInstanceId())) {
                throw new EntityNotFoundException(i18n.msg(Connection.SINK_INSTANCE_NOT_FOUND, detail.getSinkInstanceId()));
            }
            
            String connectionUniqueKey = generateConnectionUniqueKey(detail.getSourceDataCollectionId(), detail.getSinkDataCollectionId());
            
            // check connection detail DTO list duplicate records
            if (connectionUniqueKeySet.contains(connectionUniqueKey)) {
                String sinkDataCollectionName = sinkDataCollectionMapping.get(detail.getSinkDataCollectionId()).getName();
                throw new EntityExistsException(i18n.msg(Connection.ALREADY_EXISTED, sinkDataCollectionName));
            }
            
            // check exists duplicate connection
            if (alreadyExistConnectionMapping.containsKey(connectionUniqueKey)) {
                String sinkDataCollectionName = sinkDataCollectionMapping.get(detail.getSinkDataCollectionId()).getName();
                throw new EntityExistsException(i18n.msg(Connection.ALREADY_EXISTED, sinkDataCollectionName));
            }
            
            connectionUniqueKeySet.add(connectionUniqueKey);
        }
    }
    
    private Map<String, ConnectionDTO> getAlreadyExistConnectionMapping(final Set<Long> sinkDataCollectionIds) {
        ConnectionQuery connectionQuery = new ConnectionQuery()
                .setSinkDataCollectionIds(sinkDataCollectionIds)
                .setDeleted(false);
        
        List<ConnectionDTO> connectionDTOs = query(connectionQuery);
        
        Map<String, ConnectionDTO> connectionMapping = Maps.newHashMap();
        
        for (ConnectionDTO connectionDTO : connectionDTOs) {
            String key = generateConnectionUniqueKey(
                    connectionDTO.getSourceDataCollectionId(),
                    connectionDTO.getSinkDataCollectionId());
            
            connectionMapping.put(key, connectionDTO);
        }
        
        return connectionMapping;
    }
    
    private String generateConnectionUniqueKey(final Long sourceDataCollectionId, final Long sinkDataCollectionId) {
        return Joiner.on(Symbol.CABLE).join(sourceDataCollectionId, sinkDataCollectionId);
    }
    
    private Map<Long, ProjectDTO> getProjectMapping(final List<Long> projectIds) {
        if (CollectionUtils.isEmpty(projectIds)) {
            return Collections.EMPTY_MAP;
        }
        
        return projectService.getByIds(projectIds)
                .stream()
                .collect(Collectors.toMap(it -> it.getId(), it -> it));
    }
    
    private Map<Long, DataSystemResourceDTO> getDataSystemResourceMapping(final Set<Long> resourceIds) {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return Collections.EMPTY_MAP;
        }
        
        return dataSystemResourceService.getByIds(new ArrayList<>(resourceIds))
                .stream()
                .collect(Collectors.toMap(it -> it.getId(), it -> it));
    }
    
    private void checkParameterForCreateConnections(final List<ConnectionDetailDTO> connections) {
        if (CollectionUtils.isEmpty(connections)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
        }
        
        for (ConnectionDetailDTO detail : connections) {
            //            TODO 使用校验框架,校验post请求的 json body
            if (Objects.isNull(detail.getSourceProjectId())
                    || Objects.isNull(detail.getSourceDataSystemType())
                    || Objects.isNull(detail.getSourceDataCollectionId())
                    
                    || Objects.isNull(detail.getSinkProjectId())
                    || Objects.isNull(detail.getSinkDataSystemType())
                    || Objects.isNull(detail.getSinkDataCollectionId())
            ) {
                throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
            }
            
            // TODO 只需要对TIDB 类型的进行校验，MYSQL 类型默认使用 Master 实例
            if (detail.getSinkDataSystemType() == DataSystemType.TIDB) {
                if (Objects.isNull(detail.getSinkInstanceId())) {
                    throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
                }
            }
            
            // special configuration
            if (!Strings.isNullOrEmpty(detail.getSpecificConfiguration())) {
                try {
                    objectMapper.readValue(detail.getSpecificConfiguration(), Map.class);
                } catch (JsonProcessingException e) {
                    throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
                }
            }
            
            // column configurations
            if (CollectionUtils.isEmpty(detail.getConnectionColumnConfigurations())) {
                throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
            }
        }
    }
    
    private void checkParameterForUpdateConnections(final List<ConnectionDetailDTO> connections) {
        if (CollectionUtils.isEmpty(connections)) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
        }
        for (ConnectionDetailDTO detail : connections) {
            if (Objects.isNull(detail.getId())) {
                throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
            }
            // column configurations
            if (CollectionUtils.isEmpty(detail.getConnectionColumnConfigurations())) {
                throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
            }
            // special configuration
            if (!Strings.isNullOrEmpty(detail.getSpecificConfiguration())) {
                try {
                    objectMapper.readValue(detail.getSpecificConfiguration(), Map.class);
                } catch (JsonProcessingException e) {
                    throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER));
                }
            }
        }
    }
    
    @Transactional
    @Override
    public List<ConnectionDTO> batchUpdate(final List<ConnectionDetailDTO> connections) {
        checkParameterForUpdateConnections(connections);
        Set<Long> connectionIds = connections.stream().map(ConnectionDetailDTO::getId).collect(Collectors.toSet());
        Map<Long, ConnectionDO> connectionMapping = getConnectionMapping(connectionIds);
        List<ConnectionDetailDTO> toUpdateConnections = Lists.newArrayListWithCapacity(connections.size());
        
        for (ConnectionDetailDTO detail : connections) {
            if (!connectionMapping.containsKey(detail.getId())) {
                throw new EntityNotFoundException(i18n.msg(Connection.NOT_FOUND, detail.getId()));
            }
            
            ConnectionDO connectionDO = connectionMapping.get(detail.getId());
            
            // update the column configurations
            ConnectionDetailDTO toUpdateConnectionDetail = new ConnectionDetailDTO(connectionDO);
            toUpdateConnectionDetail.setConnectionColumnConfigurations(detail.getConnectionColumnConfigurations());
            toUpdateConnections.add(toUpdateConnectionDetail);
        }
        
        //delete connection old column configuration.
        List<Long> toUpdateConnectionIds = toUpdateConnections
                .stream()
                .map(it -> it.getId())
                .collect(Collectors.toList());
        connectionColumnConfigurationRepository.deleteByConnectionIdIn(toUpdateConnectionIds);
        
        // update connection and connection column configuration
        List<ConnectionDO> toSaveConnections = toUpdateConnections
                .stream()
                .map(ConnectionDetailDTO::toDO)
                .collect(Collectors.toList());
        
        return connectionRepository.saveAll(toSaveConnections)
                .stream()
                .map(ConnectionDTO::new)
                .collect(Collectors.toList());
    }
    
    private Map<Long, ConnectionDO> getConnectionMapping(final Set<Long> connectionIds) {
        if (CollectionUtils.isEmpty(connectionIds)) {
            return Collections.EMPTY_MAP;
        }
        
        return connectionRepository.findAllById(connectionIds)
                .stream()
                .collect(Collectors.toMap(it -> it.getId(), it -> it));
    }
    
    @Transactional
    @Override
    public void updateActualState(final Long connectionId, final ConnectionState state) {
        ConnectionDO connection = connectionRepository.getOne(connectionId);
        connection.setActualState(state);
        connectionRepository.save(connection);
    }
    
    @Transactional
    @Override
    public void updateDesiredState(final Long connectionId, final ConnectionState state) {
        ConnectionDO connection = connectionRepository.getOne(connectionId);
        connection.setDesiredState(state);
        connectionRepository.save(connection);
    }
    
    @Transactional
    @Override
    public void start(final Long connectionId) {
        applyConnectionConfigurationToConnector(connectionId);
        updateDesiredState(connectionId, ConnectionState.RUNNING);
    }
    
    @Transactional
    @Override
    public void stop(final Long connectionId) {
        updateDesiredState(connectionId, ConnectionState.STOPPED);
    }
    
    @Transactional
    @Override
    public ConnectionState getActualState(final Long id) {
        return connectionRepository.getOne(id).getActualState();
    }
    
    @Transactional
    @Override
    public void updateConnectionRequisitionStateByQuery(final ConnectionQuery connectionQuery, final RequisitionState requisitionState) {
        List<ConnectionDO> connections = connectionRepository.query(connectionQuery);
        connections.forEach(it -> it.setRequisitionState(requisitionState));
        
        connectionRepository.saveAll(connections);
    }
    
    @Transactional
    @Override
    public ConnectionDTO applyConnectionToConnector(final Long connectionId) {
        ConnectionDTO connection = getById(connectionId);
        
        // 1. create source
        ConnectorDetailDTO sourceConnector = createOrUpdateSourceConnector(connection);
        
        // TODO 后续优雅实现
        entityManager.flush();
        entityManager.clear();
        // 2. create sink
        ConnectorDetailDTO sinkConnector = createSinkConnector(connection);
        
        // 3. return connection
        return afterApplyConnectionToConnector(connection, sourceConnector, sinkConnector);
    }
    
    @Transactional
    @Override
    public void applyConnectionConfigurationToConnector(final Long connectionId) {
        // get connection
        ConnectionDTO connection = getById(connectionId);
        
        if (!existsSinkConnectorInConnection(connection)) {
            return;
        }
        
        // TODO currently, only the sink connector configuration will be updated, should we update source connector configuration too?
        
        // get sink connector service
        DataSystemSinkConnectorService dataSystemSinkConnectorService = dataSystemServiceManager.getDataSystemSinkConnectorService(connection.getSinkDataSystemType());
        
        // generate a custom configuration
        Map<String, String> toUpdatedConfiguration = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connection.getId());
        
        // update connector configuration
        connectorService.updateParticularConfiguration(connection.getSinkConnectorId(), toUpdatedConfiguration);
    }
    
    private boolean existsSinkConnectorInConnection(final ConnectionDTO connection) {
        return Objects.nonNull(connection.getSinkConnectorId());
    }
    
    private ConnectionDTO afterApplyConnectionToConnector(
            final ConnectionDTO connection,
            final ConnectorDetailDTO sourceConnector,
            final ConnectorDetailDTO sinkConnector
    ) {
        connection.setSourceConnectorId(sourceConnector.getId());
        connection.setSinkConnectorId(sinkConnector.getId());
        
        // TODO 后续优雅实现
        entityManager.flush();
        entityManager.clear();
        
        ConnectionDO savedConnectionDO = connectionRepository.save(connection.toDO());
        return new ConnectionDTO(savedConnectionDO);
    }
    
    protected ConnectorDetailDTO createOrUpdateSourceConnector(final ConnectionDTO connection) {
        // get data system source connector service
        Long sourceDataCollectionId = connection.getSourceDataCollectionId();
        DataSystemType dataSystemType = dataSystemResourceService.getDataSystemType(sourceDataCollectionId);
        DataSystemSourceConnectorService dataSystemSourceConnectorService = dataSystemServiceManager.getDataSystemSourceConnectorService(dataSystemType);
        DataSystemResourceType connectorDataSystemResourceType = dataSystemSourceConnectorService.getConnectorDataSystemResourceType();
        Long sourceConnectorDataSystemResourceId = getSourceConnectorDataSystemResourceId(sourceDataCollectionId, connectorDataSystemResourceType);
        // source data collection's connector, maybe exists or not.
        Optional<ConnectorDetailDTO> connectorDetailOpt = connectorService.getDetailByDataSystemResourceId(sourceConnectorDataSystemResourceId);
        
        // create kafka topic of the resource
        String resourceKafkaTopicName = dataSystemSourceConnectorService.generateKafkaTopicName(sourceDataCollectionId);
        createResourceKafkaTopicIfAbsent(sourceDataCollectionId, resourceKafkaTopicName);
        
        // if the connector already exists, update connector configuration
        if (connectorDetailOpt.isPresent()) {
            ConnectorDetailDTO alreadyExistConnectorDTO = connectorDetailOpt.get();
            
            List<ConnectionDO> sourceConnectorDataSystemResourceAlreadyAppliedConnections =
                    getSourceConnectorDataSystemResourceAlreadyAppliedConnections(alreadyExistConnectorDTO.getId());
            
            // get the source connector required data collection ids
            List<Long> sourceDataCollectionIds = getSourceConnectorNeedSubscribedDataCollectionIds(
                    sourceDataCollectionId,
                    sourceConnectorDataSystemResourceAlreadyAppliedConnections
            );
            
            // generate a custom configuration
            Map<String, String> toUpdatedConfiguration = dataSystemSourceConnectorService.generateConnectorCustomConfiguration(sourceDataCollectionIds);
            
            // discard  immutable connector config item
            Set<String> immutableConfigurationNames = dataSystemSourceConnectorService.getImmutableConfigurationNames();
            discardImmutableConnectorConfiguration(alreadyExistConnectorDTO.getId(), toUpdatedConfiguration, immutableConfigurationNames);
            
            // update configuration
            connectorService.updateParticularConfiguration(alreadyExistConnectorDTO.getId(), toUpdatedConfiguration);
            
            return connectorService.getDetailById(alreadyExistConnectorDTO.getId());
        }
        
        // verify data system meta
        dataSystemSourceConnectorService.verifyDataSystemMetadata(sourceDataCollectionId);
        
        // execute before connector creation hook
        dataSystemSourceConnectorService.beforeConnectorCreation(sourceDataCollectionId);
        
        // create source connector
        String connectorName = dataSystemSourceConnectorService.generateConnectorName(sourceDataCollectionId);
        List<Long> sourceDataCollectionIds = getSourceConnectorNeedSubscribedDataCollectionIds(sourceDataCollectionId, Collections.EMPTY_LIST);
        
        Map<String, String> connectorConfiguration = mergeConnectorDefaultConfigurationAndCustomConfiguration(
                dataSystemSourceConnectorService.getConnectorDefaultConfiguration(),
                dataSystemSourceConnectorService.generateConnectorCustomConfiguration(sourceDataCollectionIds)
        );
        
        ConnectorDetailDTO createdConnector = connectorService.create(buildConnectorDetailDTO(
                dataSystemSourceConnectorService.getConnectorClass(),
                getACDCKafkaCluster(),
                connectorName,
                connectorConfiguration,
                sourceConnectorDataSystemResourceId
        ));
        
        // execute after connector creation hook
        dataSystemSourceConnectorService.afterConnectorCreation(sourceDataCollectionId);
        
        return createdConnector;
    }
    
    private ConnectorDetailDTO buildConnectorDetailDTO(
            final ConnectorClassDetailDTO connectorClassDetailDTO,
            final KafkaClusterDTO kafkaClusterDTO,
            final String connectorName,
            final Map<String, String> connectorConfiguration,
            final Long connectorDataSystemResourceId
    ) {
        ConnectClusterDTO connectClusterDTO = chooseConnectClusterByConnectorClass(connectorClassDetailDTO);
        
        List<ConnectorConfigurationDTO> connectorConfigurations = Lists.newArrayListWithCapacity(connectorConfiguration.size());
        
        connectorConfiguration.forEach((k, v) -> connectorConfigurations.add(new ConnectorConfigurationDTO(k, v)));
        
        return new ConnectorDetailDTO()
                .setName(connectorName)
                .setDesiredState(ConnectorState.RUNNING)
                .setActualState(ConnectorState.PENDING)
                .setConnectClusterId(connectClusterDTO.getId())
                .setKafkaClusterId(kafkaClusterDTO.getId())
                .setConnectorClassId(connectorClassDetailDTO.getId())
                .setConnectorConfigurations(connectorConfigurations)
                .setDataSystemResourceId(connectorDataSystemResourceId);
    }
    
    private ConnectClusterDTO chooseConnectClusterByConnectorClass(final ConnectorClassDetailDTO connectorClassDetailDTO) {
        // TODO 后续实现 connect 集群的动态路由能力
        return connectorClassDetailDTO.getConnectClusters().get(0);
    }
    
    protected List<Long> getSourceConnectorNeedSubscribedDataCollectionIds(
            final Long sourceDataCollectionId,
            final List<ConnectionDO> sourceConnectorDataSystemResourceAlreadyAppliedConnections) {
        
        Set<Long> needSubscribedDataCollectionIds = Sets.newHashSet();
        
        for (ConnectionDO connection : sourceConnectorDataSystemResourceAlreadyAppliedConnections) {
            needSubscribedDataCollectionIds.add(connection.getSourceDataCollection().getId());
        }
        
        needSubscribedDataCollectionIds.add(sourceDataCollectionId);
        
        return new ArrayList<>(needSubscribedDataCollectionIds);
    }
    
    private Map<String, String> mergeConnectorDefaultConfigurationAndCustomConfiguration(
            final Map<String, String> defaultConfiguration,
            final Map<String, String> customConfiguration
    ) {
        Map<String, String> connectorConfiguration = Maps.newHashMap();
        
        if (!CollectionUtils.isEmpty(defaultConfiguration)) {
            connectorConfiguration.putAll(defaultConfiguration);
        }
        
        if (!CollectionUtils.isEmpty(customConfiguration)) {
            connectorConfiguration.putAll(customConfiguration);
        }
        return connectorConfiguration;
    }
    
    protected void createResourceKafkaTopicIfAbsent(final Long resourceId, final String resourceKafkaTopicName) {
        Long kafkaClusterId = kafkaClusterService.getACDCKafkaCluster().getId();
        kafkaTopicService.createDataCollectionTopicIfAbsent(resourceId, kafkaClusterId, resourceKafkaTopicName);
    }
    
    protected List<ConnectionDO> getSourceConnectorDataSystemResourceAlreadyAppliedConnections(final Long sourceConnectorId) {
        ConnectionQuery query = new ConnectionQuery()
                .setSourceConnectorId(sourceConnectorId)
                .setDeleted(Boolean.FALSE);
        
        return connectionRepository.query(query);
    }
    
    private Long getSourceConnectorDataSystemResourceId(
            final Long dataCollectionId,
            final DataSystemResourceType connectorDataSystemResourceType) {
        DataSystemResourceDTO dataCollectionResource = dataSystemResourceService.getById(dataCollectionId);
        DataSystemResourceType dataCollectionResourceType = dataCollectionResource.getResourceType();
        
        boolean connectorDataSystemResourceIsMyself = dataCollectionResourceType == connectorDataSystemResourceType;
        
        return connectorDataSystemResourceIsMyself
                ? dataCollectionResource.getId()
                : dataSystemResourceService.getParent(dataCollectionId, connectorDataSystemResourceType).getId();
    }
    
    protected ConnectorDetailDTO createSinkConnector(final ConnectionDTO connection) {
        DataSystemSinkConnectorService dataSystemSinkConnectorService = dataSystemServiceManager.getDataSystemSinkConnectorService(connection.getSinkDataSystemType());
        
        // verify data system meta data before create connector
        dataSystemSinkConnectorService.verifyDataSystemMetadata(connection.getSinkDataCollectionId());
        
        // execute before connector creation hook
        dataSystemSinkConnectorService.beforeConnectorCreation(connection.getSinkDataCollectionId());
        
        // generate connector configuration
        Map<String, String> configuration = mergeConnectorDefaultConfigurationAndCustomConfiguration(
                dataSystemSinkConnectorService.getConnectorDefaultConfiguration(),
                dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connection.getId())
        );
        
        // generate connector name
        String connectorName = dataSystemSinkConnectorService.generateConnectorName(connection.getId());
        
        // create sink connector
        ConnectorDetailDTO createdConnector = connectorService.create(buildConnectorDetailDTO(
                dataSystemSinkConnectorService.getConnectorClass(),
                getACDCKafkaCluster(),
                connectorName,
                configuration,
                connection.getSinkDataCollectionId()
        ));
        
        return createdConnector;
    }
    
    private KafkaClusterDTO getACDCKafkaCluster() {
        return kafkaClusterService.getACDCKafkaCluster();
    }
    
    private boolean isImmutableConnectorConfiguration(
            final String configurationName,
            final Set<String> immutableConnectorConfigurationSet) {
        return immutableConnectorConfigurationSet.contains(configurationName);
    }
    
    protected void discardImmutableConnectorConfiguration(
            final Long connectorId,
            final Map<String, String> newConnectorConfiguration,
            final Set<String> immutableConnectorConfigurationNames
    ) {
        Preconditions.checkNotNull(immutableConnectorConfigurationNames, "immutable connector configuration set must not be null.");
        
        List<ConnectorConfigurationDTO> oldConnectorConfigurations = connectorService.getDetailById(connectorId).getConnectorConfigurations();
        
        // discard immutable connector config item
        for (ConnectorConfigurationDTO configuration : oldConnectorConfigurations) {
            String configurationName = configuration.getName();
            if (isImmutableConnectorConfiguration(configurationName, immutableConnectorConfigurationNames)) {
                newConnectorConfiguration.remove(configurationName);
            }
        }
    }
}
