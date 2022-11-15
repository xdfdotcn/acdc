package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO.Tuple;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionEditDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationResultDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.Dataset4ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.SinkConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.SourceConnectorInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.DatasetFrom;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionVersionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionColumnConfigurationQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionInfoQuery;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.service.entity.ConnectionColumnConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectionService;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.error.NotFoundException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl.FieldMappingProcessServiceManager;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorQueryProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessServiceManager;
import cn.xdf.acdc.devops.service.process.user.UserProcessService;
import cn.xdf.acdc.devops.service.util.BizAssert;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
public class ConnectionProcessServiceImpl implements ConnectionProcessService {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private ConnectorCoreProcessService connectorCoreProcessService;

    @Autowired
    private ConnectionColumnConfigurationService connectionColumnConfigurationService;

    @Autowired
    private DatasetProcessServiceManager datasetProcessServiceManager;

    @Autowired
    private ConnectionRepository connectionRepository;

    @Autowired
    private DatasetProcessServiceManager datasetManager;

    @Autowired
    private FieldMappingProcessServiceManager fieldMappingManager;

    @Autowired
    private UserProcessService userProcessService;

    @Autowired
    private ConnectorQueryProcessService connectorQueryProcessService;

    @Override
    public List<ConnectionDetailDTO> query(final ConnectionQuery query) {
        return connectionService.query(query).stream().map(ConnectionDetailDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    public List<ConnectionDetailDTO> detailQuery(final ConnectionQuery query) {
        return connectionRepository.query(query).stream()
                .map(this::buildConnectionDetailDTOByConnectionDO)
                .collect(Collectors.toList());
    }

    private ConnectionDetailDTO buildConnectionDetailDTOByConnectionDO(final ConnectionDO connectionDO) {
        DataSystemType sourceDataSystemType = connectionDO.getSourceDataSystemType();
        Dataset4ConnectionDTO sourceDataset4ConnectionDTO = datasetManager.getService(sourceDataSystemType)
                .getSourceDataset4Connection(new ConnectionDTO(connectionDO));

        // sink
        DataSystemType sinkDataSystemType = connectionDO.getSinkDataSystemType();
        Dataset4ConnectionDTO sinkDataset4ConnectionDTO = datasetManager.getService(sinkDataSystemType)
                .getSinkDataset4Connection(new ConnectionDTO(connectionDO));
        List<FieldMappingDTO> fieldMappingDTOs = fieldMappingManager.getFieldMapping4Connection(connectionDO.getId());

        UserDTO userDTO = userProcessService.getUser(connectionDO.getUser().getId());

        // if source, sink connector is not null, get and set their name.
        if (Objects.nonNull(connectionDO.getSourceConnector()) && Objects.nonNull(connectionDO.getSinkConnector())) {
            SourceConnectorInfoDTO sourceConnector = connectorQueryProcessService.getSourceInfo(connectionDO.getSourceConnector().getId());
            SinkConnectorInfoDTO sinkConnector = connectorQueryProcessService.getSinkInfo(connectionDO.getSinkConnector().getId());
            return new ConnectionDetailDTO(connectionDO, sourceDataset4ConnectionDTO, sinkDataset4ConnectionDTO,
                    fieldMappingDTOs, userDTO, sourceConnector, sinkConnector);
        }

        return new ConnectionDetailDTO(connectionDO, sourceDataset4ConnectionDTO, sinkDataset4ConnectionDTO,
                fieldMappingDTOs, userDTO);
    }

    @Override
    public Page<ConnectionDetailDTO> pagingQuery(final ConnectionQuery query) {
        Pageable pageable = PagedQuery.ofPage(query.getCurrent(), query.getPageSize());
        return connectionService.pagingQuery(query, pageable).map(it -> new ConnectionDetailDTO(it));
    }

    @Override
    public ConnectionDetailDTO getConnectionDetail(final Long id) {
        return connectionRepository.findById(id).map(this::buildConnectionDetailDTOByConnectionDO)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", id)));
    }

    @Override
    public void deleteConnection(final Long id) {
        ConnectionDO connectionDO = connectionRepository.findById(id)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", id)));
        connectionDO.setDeleted(true);
        connectionDO.setDesiredState(ConnectionState.STOPPED);
        connectionRepository.save(connectionDO);
    }

    @Override
    public void bulkDeleteConnection(final Set<Long> ids) {
        List<ConnectionDO> connections = connectionRepository.findAllById(ids);
        for (ConnectionDO connection : connections) {
            connection.setDeleted(Boolean.TRUE);
        }
        connectionRepository.saveAll(connections);
    }

    @Override
    public Page<ConnectionInfoDTO> detailPagingQuery(final DataSystemType dataSystemType, final ConnectionInfoQuery query, final String domainAccount) {
        Pageable pageable = PagedQuery.ofPage(query.getCurrent(), query.getPageSize());
        return connectionService.detailPagingQuery(dataSystemType, query, pageable, domainAccount)
                .map(ConnectionInfoDTO::toConnectionInfoDTO);
    }

    @Override
    public ConnectionDetailDTO applyConnectionToConnector(final ConnectionDetailDTO connection) {
        BizAssert.innerError(Objects.nonNull(connection.getId()), "ID must not be null");

        ConnectionDO dbConnection = connectionService.findById(connection.getId())
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connection.getId())));
        // query field mapping
        ConnectionColumnConfigurationQuery query = ConnectionColumnConfigurationQuery.builder()
                .connectionId(dbConnection.getId())
                .version(dbConnection.getVersion())
                .build();

        ConnectionDetailDTO toApplyConnection = new ConnectionDetailDTO(dbConnection);
        List<FieldMappingDTO> fieldMappings = connectionColumnConfigurationService.query(query).stream()
                .map(FieldMappingDTO::toFieldMapping).collect(Collectors.toList());
        toApplyConnection.setConnectionColumnConfigurations(fieldMappings);

        ConnectorCreationResultDTO result = connectorCoreProcessService
                .createSinkAndInitSource(toApplyConnection.toConnectorCreationDTO());

        dbConnection.setSinkConnector(new ConnectorDO().setId(result.getSinkConnector().getId()));
        dbConnection.setSourceConnector(new ConnectorDO().setId(result.getSourceConnector().getId()));
        ConnectionDO savedConnection = connectionService.save(dbConnection);
        return new ConnectionDetailDTO(savedConnection);
    }

    @Override
    public void flushConnectionConfigToConnector(final ConnectionDetailDTO connection) {
        BizAssert.innerError(Objects.nonNull(connection.getId()), "ID must not be null");

        ConnectionDO dbConnection = connectionService.findById(connection.getId())
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connection.getId())));

        // use db connection newest version
        ConnectionColumnConfigurationQuery query = ConnectionColumnConfigurationQuery.builder()
                .connectionId(dbConnection.getId())
                .version(dbConnection.getVersion())
                .build();

        List<FieldMappingDTO> fieldMappings = connectionColumnConfigurationService.query(query).stream()
                .map(FieldMappingDTO::toFieldMapping).collect(Collectors.toList());

        connectorCoreProcessService.editFieldMapping(dbConnection.getSinkConnector().getId(), fieldMappings);
    }

    @Override
    public List<ConnectionDTO> bulkCreateConnection(final List<ConnectionDetailDTO> connections, final String domainAccount) {
        List<ConnectionDetailDTO> toSaveConnections = new ArrayList(connections);
        List<Long> sinkDatasetIds = toSaveConnections.stream().map(ConnectionDetailDTO::getSinkDataSetId)
                .collect(Collectors.toList());
        BizAssert.alreadyExists(
                !connectionService.existsEachInSinkDatasetIds(sinkDatasetIds),
                ErrorMsg.E_102,
                String.format("Dataset already exists, sinkDatasetIds: %s", sinkDatasetIds));

        Map<DataSystemType, List<DataSetDTO>> sourceDatasetMap = new HashMap<>();
        Map<DataSystemType, List<DataSetDTO>> sinkDatasetMap = new HashMap<>();
        toSaveConnections.forEach(it -> {
            sourceDatasetMap
                    .computeIfAbsent(it.getSourceDataSystemType(), key -> Lists.newArrayList())
                    .add(it.getSourceDataSet());
            sinkDatasetMap
                    .computeIfAbsent(it.getSinkDataSystemType(), key -> Lists.newArrayList())
                    .add(it.getSinkDataSet());
        });

        // 1. 根据 DataSystemType 分批批量校验
        sourceDatasetMap.entrySet()
                .forEach(it -> datasetProcessServiceManager.getService(it.getKey())
                        .verifyDataset(it.getValue(), DatasetFrom.SOURCE));

        sinkDatasetMap.entrySet()
                .forEach(it -> datasetProcessServiceManager.getService(it.getKey())
                        .verifyDataset(it.getValue(), DatasetFrom.SINK));

        // 为 user 相关属性赋值
        UserDO currentUser = userProcessService.getUserByDomainAccount(domainAccount).toUserDO();
        toSaveConnections.stream().forEach(it -> {
            it.setUserId(currentUser.getId());
            it.setUserEmail(currentUser.getEmail());
        });

        List<ConnectionDetailDTO.Tuple> connectionTuples = toSaveConnections.stream().map(it -> new Tuple(it.toConnectionDO(),
                FieldMappingDTO.toConnectionColumnConfiguration(it.getConnectionColumnConfigurations())))
                .collect(Collectors.toList());

        return bulkSaveConnection(connectionTuples);
    }

    @Override
    public void bulkEditConnection(final List<ConnectionEditDTO> connections) {
        Map<Long, ConnectionEditDTO> toEditConnectionMap = connections.stream()
                .collect(Collectors.toMap(it -> it.getConnectionId(), it -> it));

        Set<Long> toEditConnectionIds = toEditConnectionMap.keySet();
        List<ConnectionDO> dbConnections = connectionService.findAllById(toEditConnectionIds);
        BizAssert.notFound(
                dbConnections.size() == connections.size(),
                ErrorMsg.E_110,
                String.format("Not found connection, ids: %s", toEditConnectionIds)
        );

        List<ConnectionDetailDTO.Tuple> connectionTuples = new ArrayList<>();
        dbConnections.forEach(it -> {
            List<FieldMappingDTO> fieldMappings = toEditConnectionMap.get(it.getId()).getFieldMappings();
            List<ConnectionColumnConfigurationDO> toSaveColumnConfigs = FieldMappingDTO
                    .toConnectionColumnConfiguration(fieldMappings);
            // ++ version
            it.setVersion(ConnectionVersionDO.of(it.getVersion()).incrementVersion());
            connectionTuples.add(new Tuple(it, toSaveColumnConfigs));
        });

        bulkSaveConnection(connectionTuples);
    }

    @Override
    public void editActualState(final Long connectionId, final ConnectionState state) {
        connectionService.updateActualState(connectionId, state);
    }

    @Override
    public void editDesiredState(final Long connectionId, final ConnectionState state) {
        connectionService.updateDesiredState(connectionId, state);
    }

    @Override
    public ConnectionState getActualState(final Long connectionId) {
        ConnectionDO connection = connectionService.findById(connectionId)
                .orElseThrow(() -> new NotFoundException(String.format("connectionId: %s", connectionId)));
        return connection.getActualState();
    }

    @Override
    public void bulkEditConnectionRequisitionStateByQuery(final ConnectionQuery connectionQuery, final RequisitionState requisitionState) {
        List<ConnectionDO> connections = connectionRepository.query(connectionQuery);
        connections.forEach(it -> it.setRequisitionState(requisitionState));

        connectionRepository.saveAll(connections);
    }

    private List<ConnectionDTO> bulkSaveConnection(final List<ConnectionDetailDTO.Tuple> tuples) {

        // 2. 批量保存 connection version+1
        List<ConnectionDO> savedConnections = connectionService.saveAll(
                tuples.stream().map(Tuple::getConnection).collect(Collectors.toList())
        );

        BizAssert.innerError(savedConnections.size() == tuples.size(), String.format("Bulk connection fail"));

        // 3. 批量保存配置 version+1
        List<ConnectionColumnConfigurationDO> toSaveColumnConfigs = Lists.newArrayList();
        for (int i = 0; i < savedConnections.size(); i++) {
            ConnectionDO savedConnection = savedConnections.get(i);
            for (ConnectionColumnConfigurationDO conf : tuples.get(i).getColumnConfigs()) {
                conf.setConnection(new ConnectionDO(savedConnection.getId()));
                conf.setConnectionVersion(savedConnection.getVersion());
            }
            toSaveColumnConfigs.addAll(tuples.get(i).getColumnConfigs());
        }

        connectionColumnConfigurationService.saveAll(toSaveColumnConfigs);

        return savedConnections.stream().map(ConnectionDTO::new).collect(Collectors.toList());
    }
}
