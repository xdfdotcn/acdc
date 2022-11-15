package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionEditDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorCreationResultDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSetDTO;
import cn.xdf.acdc.devops.core.domain.dto.FieldMappingDTO;
import cn.xdf.acdc.devops.core.domain.dto.enumeration.DatasetFrom;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionColumnConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionColumnConfigurationQuery;
import cn.xdf.acdc.devops.service.BaseTest;
import cn.xdf.acdc.devops.service.entity.ConnectionColumnConfigurationService;
import cn.xdf.acdc.devops.service.entity.ConnectionService;
import cn.xdf.acdc.devops.service.error.AlreadyExistsException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorCoreProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.hive.impl.HiveDatasetProcessServiceImpl;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.impl.KafkaDatasetProcessServiceImpl;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.impl.MysqlDatasetProcessServiceImpl;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.impl.TidbDatasetProcessServiceImpl;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionProcessServiceImplTest {

    @Mock
    private ConnectionService connectionService;

    @Mock
    private ConnectorCoreProcessService connectorCoreProcessService;

    @Mock
    private ConnectionColumnConfigurationService connectionColumnConfigurationService;

    @Mock
    private MysqlDatasetProcessServiceImpl mysqlDatasetProcessServiceImpl;

    @Mock
    private TidbDatasetProcessServiceImpl tidbDatasetProcessServiceImpl;

    @Mock
    private HiveDatasetProcessServiceImpl hiveDatasetProcessServiceImpl;

    @Mock
    private KafkaDatasetProcessServiceImpl kafkaDatasetProcessServiceImpl;

    private ConnectionProcessService connectionProcessService;

    @Before
    public void setup() {
        when(mysqlDatasetProcessServiceImpl.dataSystemType()).thenReturn(DataSystemType.MYSQL);
        when(tidbDatasetProcessServiceImpl.dataSystemType()).thenReturn(DataSystemType.TIDB);
        when(hiveDatasetProcessServiceImpl.dataSystemType()).thenReturn(DataSystemType.HIVE);
        when(kafkaDatasetProcessServiceImpl.dataSystemType()).thenReturn(DataSystemType.KAFKA);
        DatasetProcessServiceManager datasetProcessServiceManager = new DatasetProcessServiceManager(
                Lists.newArrayList(
                        mysqlDatasetProcessServiceImpl,
                        tidbDatasetProcessServiceImpl,
                        hiveDatasetProcessServiceImpl,
                        kafkaDatasetProcessServiceImpl
                )
        );
        connectionProcessService = new ConnectionProcessServiceImpl();
        ReflectionTestUtils.setField(connectionProcessService, "connectionService", connectionService);
        ReflectionTestUtils.setField(connectionProcessService, "connectorCoreProcessService", connectorCoreProcessService);
        ReflectionTestUtils.setField(connectionProcessService, "connectionColumnConfigurationService", connectionColumnConfigurationService);
        ReflectionTestUtils.setField(connectionProcessService, "datasetProcessServiceManager", datasetProcessServiceManager);
    }

    @Test
    public void testApplyConnectionToConnector() {
        ConnectionDO connectionDO = createConnectionDO();
        List<ConnectionColumnConfigurationDO> columnConfigs = createColumnConfigs();
        List<FieldMappingDTO> fieldMappings = createFieldMappings(columnConfigs);
        ConnectorCreationResultDTO connectorCreationResult = ConnectorCreationResultDTO.builder()
                .sourceConnector(ConnectorDTO.builder().id(60L).build())
                .sinkConnector(ConnectorDTO.builder().id(70L).build())
                .build();
        when(connectionService.findById(99L)).thenReturn(Optional.of(connectionDO));
        when(connectionService.save(any())).thenReturn(connectionDO);
        when(connectionColumnConfigurationService.query(any())).thenReturn(columnConfigs);
        when(connectorCoreProcessService.createSinkAndInitSource(any())).thenReturn(connectorCreationResult);

        ConnectionDetailDTO connectionDetailDTO = ConnectionDetailDTO.builder().id(99L).build();
        connectionProcessService.applyConnectionToConnector(connectionDetailDTO);

        // query
        ArgumentCaptor<ConnectionColumnConfigurationQuery> queryCaptor = ArgumentCaptor.forClass(ConnectionColumnConfigurationQuery.class);
        Mockito.verify(connectionColumnConfigurationService).query(queryCaptor.capture());
        Assertions.assertThat(99L).isEqualTo(queryCaptor.getValue().getConnectionId());
        Assertions.assertThat(2).isEqualTo(queryCaptor.getValue().getVersion());

        // connectCoreProcessService.createSinkAndInitSource
        ArgumentCaptor<ConnectorCreationDTO> connectorCreationCaptor = ArgumentCaptor.forClass(ConnectorCreationDTO.class);
        Mockito.verify(connectorCoreProcessService).createSinkAndInitSource(connectorCreationCaptor.capture());
        ConnectorCreationDTO expectConnectorCreation = ConnectorCreationDTO.builder()
                .sourceDataset(DataSetDTO.builder().projectId(1L).dataSetId(3L).dataSystemType(DataSystemType.MYSQL).build())
                .sinkDataset(DataSetDTO.builder().projectId(2L).dataSetId(4L).instanceId(5L).dataSystemType(DataSystemType.MYSQL).build())
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sinkDataSystemType(DataSystemType.MYSQL)
                .fieldMappings(fieldMappings)
                .build();

        Assertions.assertThat(connectorCreationCaptor.getValue()).isEqualTo(expectConnectorCreation);

        // connectionService.save
        connectionDO.setSourceConnector(new ConnectorDO(60L));
        connectionDO.setSinkConnector(new ConnectorDO(70L));
        ArgumentCaptor<ConnectionDO> connectionCaptor = ArgumentCaptor.forClass(ConnectionDO.class);
        Mockito.verify(connectionService).save(connectionCaptor.capture());
        Assertions.assertThat(connectionCaptor.getValue()).isEqualTo(connectionDO);
    }

    @Test
    public void testFlushConnectionConfigToConnector() {
        ConnectionDO connectionDO = createConnectionDO();
        connectionDO.setSourceConnector(new ConnectorDO(60L));
        connectionDO.setSinkConnector(new ConnectorDO(70L));
        List<ConnectionColumnConfigurationDO> columnConfigs = createColumnConfigs();
        List<FieldMappingDTO> fieldMappings = createFieldMappings(columnConfigs);
        when(connectionService.findById(99L)).thenReturn(Optional.of(connectionDO));
        when(connectionColumnConfigurationService.query(any())).thenReturn(columnConfigs);

        ConnectionDetailDTO connectionDetailDTO = ConnectionDetailDTO.builder().id(99L).build();
        connectionProcessService.flushConnectionConfigToConnector(connectionDetailDTO);

        // query
        ArgumentCaptor<ConnectionColumnConfigurationQuery> queryCaptor = ArgumentCaptor.forClass(ConnectionColumnConfigurationQuery.class);
        Mockito.verify(connectionColumnConfigurationService).query(queryCaptor.capture());
        Assertions.assertThat(99L).isEqualTo(queryCaptor.getValue().getConnectionId());
        Assertions.assertThat(2).isEqualTo(queryCaptor.getValue().getVersion());

        // edit

        ArgumentCaptor<Long> sinkConnectorIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<List<FieldMappingDTO>> fieldMappingsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(connectorCoreProcessService).editFieldMapping(sinkConnectorIdCaptor.capture(), fieldMappingsCaptor.capture());
        Assertions.assertThat(sinkConnectorIdCaptor.getValue()).isEqualTo(70L);
        Assertions.assertThat(fieldMappingsCaptor.getValue()).isEqualTo(fieldMappings);
    }

    @Test
    public void testBulkCreateConnectionShouldSuccess() {
        List<ConnectionDetailDTO> connections = createConnectionDTOs();
        List<ConnectionDO> toSaveConnections = connections.stream().map(ConnectionDetailDTO::toConnectionDO).collect(Collectors.toList());

        List<ConnectionDO> savedConnections = connections.stream().map(ConnectionDetailDTO::toConnectionDO).collect(Collectors.toList());
        AtomicLong idIndex = new AtomicLong(1);
        savedConnections.forEach(it -> it.setId(idIndex.incrementAndGet()));

        when(connectionService.existsEachInSinkDatasetIds(any())).thenReturn(false);

        when(connectionService.saveAll(any())).thenReturn(savedConnections);

        connectionProcessService.bulkCreateConnection(connections, BaseTest.TEST_DOMAIN_ACCOUNT);
        Mockito.verify(mysqlDatasetProcessServiceImpl, Mockito.times(1)).verifyDataset(any(), eq(DatasetFrom.SINK));
        Mockito.verify(mysqlDatasetProcessServiceImpl, Mockito.times(1)).verifyDataset(any(), eq(DatasetFrom.SOURCE));

        Mockito.verify(tidbDatasetProcessServiceImpl, Mockito.times(1)).verifyDataset(any(), eq(DatasetFrom.SINK));
        Mockito.verify(tidbDatasetProcessServiceImpl, Mockito.times(1)).verifyDataset(any(), eq(DatasetFrom.SOURCE));

        Mockito.verify(hiveDatasetProcessServiceImpl, Mockito.times(1)).verifyDataset(any(), eq(DatasetFrom.SINK));

        Mockito.verify(kafkaDatasetProcessServiceImpl, Mockito.times(1)).verifyDataset(any(), eq(DatasetFrom.SINK));

        //save
        ArgumentCaptor<List<ConnectionDO>> connectionSaveAllCapture = ArgumentCaptor.forClass(List.class);
        Mockito.verify(connectionService).saveAll(connectionSaveAllCapture.capture());

        connectionSaveAllCapture.getValue().forEach(it -> {
            it.setCreationTime(null);
            it.setUpdateTime(null);
        });

        toSaveConnections.forEach(it -> {
            it.setCreationTime(null);
            it.setUpdateTime(null);
        });

        Assertions.assertThat(connectionSaveAllCapture.getValue()).isEqualTo(toSaveConnections);

        ArgumentCaptor<List<ConnectionColumnConfigurationDO>> configSaveAllCapture = ArgumentCaptor.forClass(List.class);
        Mockito.verify(connectionColumnConfigurationService).saveAll(configSaveAllCapture.capture());

        Map<Long, List<ConnectionColumnConfigurationDO>> columnConfMap = configSaveAllCapture.getValue().stream()
                .collect(Collectors.groupingBy(it -> it.getConnection().getId(), Collectors.toList()));

        // first create, the version should be 1
        savedConnections.forEach(connection -> columnConfMap.get(connection.getId()).forEach(conf -> {
            Assertions.assertThat(conf.getConnection().getId()).isEqualTo(connection.getId());
            Assertions.assertThat(conf.getConnectionVersion()).isEqualTo(connection.getVersion());
            Assertions.assertThat(conf.getConnectionVersion()).isEqualTo(1);
        }));
    }

    @Test(expected = AlreadyExistsException.class)
    public void testBulkCreateConnectionShouldFailWhenExistingSinkDataSet() {
        List<ConnectionDetailDTO> connections = createConnectionDTOs();
        when(connectionService.existsEachInSinkDatasetIds(any())).thenReturn(true);
        connectionProcessService.bulkCreateConnection(connections, BaseTest.TEST_DOMAIN_ACCOUNT);
    }

    @Test
    public void testBulkEditConnection() {
        List<ConnectionEditDTO> connectionEditDTOs = createConnectionEditDTOs();
        List<ConnectionDO> dbConnections = createConnectionDTOs().stream().map(ConnectionDetailDTO::toConnectionDO).collect(Collectors.toList());
        for (int i = 0; i < dbConnections.size(); i++) {
            connectionEditDTOs.get(i).setConnectionId(Long.valueOf(i));
            dbConnections.get(i).setId(Long.valueOf(i));
        }
        when(connectionService.findAllById(any())).thenReturn(dbConnections);
        when(connectionService.saveAll(any())).thenReturn(dbConnections);
        connectionProcessService.bulkEditConnection(connectionEditDTOs);

        ArgumentCaptor<List<ConnectionColumnConfigurationDO>> configSaveAllCapture = ArgumentCaptor.forClass(List.class);
        Mockito.verify(connectionColumnConfigurationService).saveAll(configSaveAllCapture.capture());

        Map<Long, List<ConnectionColumnConfigurationDO>> columnConfMap = configSaveAllCapture.getValue().stream()
                .collect(Collectors.groupingBy(it -> it.getConnection().getId(), Collectors.toList()));

        // first create, the version should be 1
        dbConnections.forEach(connection -> columnConfMap.get(connection.getId()).forEach(conf -> {
            Assertions.assertThat(conf.getConnection().getId()).isEqualTo(connection.getId());
            Assertions.assertThat(conf.getConnectionVersion()).isEqualTo(connection.getVersion());
            Assertions.assertThat(conf.getConnectionVersion()).isEqualTo(2);
        }));
    }

    private ConnectionDO createConnectionDO() {
        return ConnectionDO.builder()
                .id(99L)
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sinkDataSystemType(DataSystemType.MYSQL)

                .sourceConnector(new ConnectorDO(1L))
                .sinkConnector(new ConnectorDO(2L))

                .sourceProject(new ProjectDO(1L))
                .sinkProject(new ProjectDO(2L))

                .sourceDataSetId(3L)
                .sinkDataSetId(4L)
                .sinkInstanceId(5L)

                .version(2)
                .requisitionState(RequisitionState.APPROVING)
                .desiredState(ConnectionState.STOPPED)
                .actualState(ConnectionState.STOPPED).build();
    }

    private List<ConnectionColumnConfigurationDO> createColumnConfigs() {
        return Lists.newArrayList(
                ConnectionColumnConfigurationDO.builder()
                        .filterOperator("!=")
                        .filterValue("10")
                        .sourceColumnName("id\tbit\tPRI")
                        .sinkColumnName("tid\tbit\tPRI")
                        .build(),
                ConnectionColumnConfigurationDO.builder()
                        .filterOperator(">")
                        .filterValue("20")
                        .sourceColumnName("sid\tbit\tPRI")
                        .sinkColumnName("fid\tbit\tPRI")
                        .build()
        );
    }

    private List<FieldMappingDTO> createFieldMappings(final List<ConnectionColumnConfigurationDO> columnConfigs) {
        return columnConfigs.stream().map(FieldMappingDTO::toFieldMapping).collect(Collectors.toList());
    }

    private ConnectionDO cloneOf(final ConnectionDO connectionDO) {
        return ConnectionDO.builder()
                .id(connectionDO.getId())
                .sourceDataSystemType(connectionDO.getSourceDataSystemType())
                .sourceProject(connectionDO.getSourceProject())
                .sourceDataSetId(connectionDO.getSourceDataSetId())
                .sourceConnector(connectionDO.getSourceConnector())
                .sinkDataSystemType(connectionDO.getSinkDataSystemType())
                .sinkProject(connectionDO.getSinkProject())
                .sinkInstanceId(connectionDO.getSinkInstanceId())
                .sinkDataSetId(connectionDO.getSinkDataSetId())
                .sinkConnector(connectionDO.getSinkConnector())
                .specificConfiguration(connectionDO.getSpecificConfiguration())
                .version(connectionDO.getVersion())
                .requisitionState(connectionDO.getRequisitionState())
                .desiredState(connectionDO.getDesiredState())
                .actualState(connectionDO.getDesiredState())
                .user(connectionDO.getUser())
                .deleted(connectionDO.getDeleted())
                .updateTime(connectionDO.getUpdateTime())
                .creationTime(connectionDO.getCreationTime())
                .build();
    }

    private List<ConnectionDetailDTO> createConnectionDTOs() {

        List<FieldMappingDTO> fieldMappings = createFieldMappings(createColumnConfigs());
        return Lists.newArrayList(
                // mysql -> mysql
                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.MYSQL)
                        .sourceProjectId(101L)
                        .sourceDataSetId(102L)
                        .sinkDataSystemType(DataSystemType.MYSQL)
                        .sinkProjectId(103L)
                        .sinkDataSetId(104L)
                        .sinkInstanceId(105L)
                        .userId(BaseTest.TEST_USER_ID)
                        .connectionColumnConfigurations(fieldMappings)
                        .build(),
                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.TIDB)
                        .sourceProjectId(201L)
                        .sourceDataSetId(202L)
                        .sinkDataSystemType(DataSystemType.TIDB)
                        .sinkProjectId(203L)
                        .sinkDataSetId(204L)
                        .sinkInstanceId(205L)
                        .userId(BaseTest.TEST_USER_ID)
                        .connectionColumnConfigurations(fieldMappings)
                        .build(),
                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.TIDB)
                        .sourceProjectId(301L)
                        .sourceDataSetId(302L)
                        .sinkDataSystemType(DataSystemType.HIVE)
                        .sinkProjectId(303L)
                        .sinkDataSetId(304L)
                        .sinkInstanceId(305L)
                        .userId(BaseTest.TEST_USER_ID)
                        .connectionColumnConfigurations(fieldMappings)
                        .build(),
                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.TIDB)
                        .sourceProjectId(401L)
                        .sourceDataSetId(402L)
                        .sinkDataSystemType(DataSystemType.KAFKA)
                        .sinkProjectId(403L)
                        .sinkDataSetId(404L)
                        .sinkInstanceId(405L)
                        .userId(BaseTest.TEST_USER_ID)
                        .connectionColumnConfigurations(fieldMappings)
                        .specificConfiguration("{}")
                        .build()
        );
    }

    private List<ConnectionEditDTO> createConnectionEditDTOs() {

        List<FieldMappingDTO> fieldMappings = createFieldMappings(createColumnConfigs());
        return Lists.newArrayList(
                // mysql -> mysql
                ConnectionEditDTO.builder()
                        .fieldMappings(fieldMappings)
                        .build(),
                ConnectionEditDTO.builder()
                        .fieldMappings(fieldMappings)
                        .build(),
                ConnectionEditDTO.builder()
                        .fieldMappings(fieldMappings)
                        .build(),
                ConnectionEditDTO.builder()
                        .fieldMappings(fieldMappings)
                        .build()
        );
    }

}
