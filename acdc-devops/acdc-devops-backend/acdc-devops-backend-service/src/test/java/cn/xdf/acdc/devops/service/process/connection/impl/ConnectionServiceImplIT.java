package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.core.domain.dto.ConnectClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.AuthorityRoleType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RequisitionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectorState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.core.util.StringUtil;
import cn.xdf.acdc.devops.repository.AuthorityRepository;
import cn.xdf.acdc.devops.repository.ConnectionRepository;
import cn.xdf.acdc.devops.repository.DataSystemResourceRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class ConnectionServiceImplIT {

    private static final String DOMAIN_ACCOUNT_ADMIN = "admin";

    private static final String DOMAIN_ACCOUNT_NORMAL = "normal";

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private AuthorityRepository authorityRepository;

    // mock bean
    private ConnectionRepository mockConnectionRepository;

    private DataSystemResourceRepository mockDataSystemResourceRepository;

    private DataSystemResourceService mockDataSystemResourceService;

    private DataSystemServiceManager mockDataSystemServiceManager;

    private ConnectorService mockConnectorService;

    private DataSystemSourceConnectorService mockDataSystemSourceConnectorService;

    private DataSystemSinkConnectorService mockDataSystemSinkConnectorService;

    private KafkaClusterService mockKafkaClusterService;

    private KafkaTopicService mockKafkaTopicService;

    @Before
    public void setup() {
        createAuthorityRoles();
        createAdminUser();
        createNormalUser();
        mockConnectionRepository = Mockito.mock(ConnectionRepository.class);
        mockDataSystemResourceService = Mockito.mock(DataSystemResourceService.class);
        mockDataSystemServiceManager = Mockito.mock(DataSystemServiceManager.class);
        mockConnectorService = Mockito.mock(ConnectorService.class);
        mockDataSystemSourceConnectorService = Mockito.mock(DataSystemSourceConnectorService.class);
        mockDataSystemSinkConnectorService = Mockito.mock(DataSystemSinkConnectorService.class);
        mockKafkaClusterService = Mockito.mock(KafkaClusterService.class);
        mockKafkaTopicService = Mockito.mock(KafkaTopicService.class);
        mockDataSystemResourceRepository = Mockito.mock(DataSystemResourceRepository.class);
    }

    @Test
    public void testCreate() {
        List<ConnectionDetailDTO> toSaveConnectionDTOs = createConnectionDetailLimit2();
        List<ConnectionDetailDTO> savedConnectionDTOs = connectionService.batchCreate(toSaveConnectionDTOs, DOMAIN_ACCOUNT_NORMAL);
        Assertions.assertThat(savedConnectionDTOs.size()).isEqualTo(toSaveConnectionDTOs.size());

        // verify  source
        verifySavedConnection(savedConnectionDTOs.get(0), 1, false);

        // verify sink
        verifySavedConnection(savedConnectionDTOs.get(1), 1, true);

        // check sink
        Assertions.assertThat(savedConnectionDTOs.get(1).getVersion()).isEqualTo(1);

        Map<String, ConnectionColumnConfigurationDTO> columnConfiguration1 = savedConnectionDTOs.get(0).getConnectionColumnConfigurations()
                .stream()
                .collect(Collectors.toMap(it -> it.getSinkColumnName(), it -> it));

        Assertions.assertThat(columnConfiguration1.size()).isEqualTo(3);
        Assertions.assertThat(columnConfiguration1.get("b1").getSourceColumnName()).isEqualTo("a1");
        Assertions.assertThat(columnConfiguration1.get("b1").getId()).isNotNull();
        Assertions.assertThat(columnConfiguration1.get("b1").getSourceColumnType()).isEqualTo("bigint(20)");
        Assertions.assertThat(columnConfiguration1.get("b1").getSourceColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("PRIMARY"));

        Assertions.assertThat(columnConfiguration1.get("b1").getSinkColumnName()).isEqualTo("b1");
        Assertions.assertThat(columnConfiguration1.get("b1").getSinkColumnType()).isEqualTo("bigint(50)");
        Assertions.assertThat(columnConfiguration1.get("b1").getSinkColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("PRIMARY"));

        Assertions.assertThat(columnConfiguration1.get("b2").getFilterOperator()).isEqualTo(">");
        Assertions.assertThat(columnConfiguration1.get("b2").getId()).isNotNull();
        Assertions.assertThat(columnConfiguration1.get("b2").getFilterValue()).isEqualTo("10");

        Map<String, ConnectionColumnConfigurationDTO> columnConfiguration2 = savedConnectionDTOs.get(1).getConnectionColumnConfigurations()
                .stream()
                .collect(Collectors.toMap(it -> it.getSinkColumnName(), it -> it));

        Assertions.assertThat(columnConfiguration2.size()).isEqualTo(3);
        Assertions.assertThat(columnConfiguration2.get("d1").getSourceColumnName()).isEqualTo("c1");
        Assertions.assertThat(columnConfiguration2.get("d1").getSourceColumnType()).isEqualTo("bigint(20)");
        Assertions.assertThat(columnConfiguration2.get("d1").getSourceColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("PRIMARY"));

        Assertions.assertThat(columnConfiguration2.get("d1").getSinkColumnName()).isEqualTo("d1");
        Assertions.assertThat(columnConfiguration2.get("d1").getSinkColumnType()).isEqualTo("bigint(50)");
        Assertions.assertThat(columnConfiguration2.get("d1").getSinkColumnUniqueIndexNames()).isEqualTo(Sets.newHashSet("PRIMARY"));

        Assertions.assertThat(columnConfiguration2.get("d2").getFilterOperator()).isEqualTo(">=");
        Assertions.assertThat(columnConfiguration2.get("d2").getFilterValue()).isEqualTo("10");

    }

    @Test(expected = EntityExistsException.class)
    public void testCreateShouldThrowExceptionWhenAlreadyExistConnection() {
        ConnectionDetailDTO toSaveConnectionDTO = createConnectionDetailLimit2().get(0);
        connectionService.batchCreate(Lists.newArrayList(toSaveConnectionDTO), DOMAIN_ACCOUNT_NORMAL);
        connectionService.batchCreate(Lists.newArrayList(toSaveConnectionDTO), DOMAIN_ACCOUNT_NORMAL);
    }

    @Test(expected = EntityExistsException.class)
    public void testCreateShouldThrowExceptionWhenExistSameConnection() {
        List<ConnectionDetailDTO> toSaveConnectionDTOs = createSameConnectionDetailLimit2();
        connectionService.batchCreate(toSaveConnectionDTOs, DOMAIN_ACCOUNT_NORMAL);
    }

    @Test
    public void testCreateShouldSuccessWhenDifferentSourceDataCollectionToSameSinkDataCollection() {
        List<ConnectionDetailDTO> fakeConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO toSaveConnectionDTO1 = fakeConnectionDetailDTOs.get(0);
        connectionService.batchCreate(Lists.newArrayList(toSaveConnectionDTO1), DOMAIN_ACCOUNT_ADMIN);

        ConnectionDetailDTO toSaveConnectionDTO2 = fakeConnectionDetailDTOs.get(1);
        toSaveConnectionDTO2.setSinkDataCollectionId(toSaveConnectionDTO1.getSinkDataCollectionId());
        connectionService.batchCreate(Lists.newArrayList(toSaveConnectionDTO2), DOMAIN_ACCOUNT_ADMIN);
    }

    @Test
    public void testCreateShouldThrowExceptionWhenIllegalParameter() {
        ConnectionDetailDTO connectionDetailDTO = createConnectionDetail();
        Assertions.assertThat(Assertions.catchThrowable(() -> connectionService
                        .batchCreate(Lists.newArrayList(), DOMAIN_ACCOUNT_ADMIN)))
                .isInstanceOf(ClientErrorException.class);

        // source project id required
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSourceProjectId(null))))
                .isInstanceOf(ClientErrorException.class);

        // source data system type required
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSourceDataSystemType(null)
        ))).isInstanceOf(ClientErrorException.class);

        // source data collection id required
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSourceDataCollectionId(null)
        ))).isInstanceOf(ClientErrorException.class);

        // sink project id required
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkProjectId(null)
        ))).isInstanceOf(ClientErrorException.class);

        // sink data system type required
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkDataSystemType(null)
        ))).isInstanceOf(ClientErrorException.class);

        // sink data collection id required
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkDataCollectionId(null)
        ))).isInstanceOf(ClientErrorException.class);

        // special configuration json format
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSpecificConfiguration("666")
        ))).isInstanceOf(ClientErrorException.class);

        // column configurations
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setConnectionColumnConfigurations(null)
        ))).isInstanceOf(ClientErrorException.class);
    }

    @Test
    public void testCreateShouldThrowExceptionWhenIllegalMetaId() {
        ConnectionDetailDTO connectionDetailDTO = createConnectionDetailLimit2().get(0);

        // source project must exist
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSourceProjectId(-99L)
        ))).isInstanceOf(EntityNotFoundException.class);

        // sink project must exist
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkProjectId(-99L)
        ))).isInstanceOf(EntityNotFoundException.class);

        // source project owner must exist
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkProjectId(saveProjectWithoutOwner("source_prj_test").getId())
        ))).isInstanceOf(EntityNotFoundException.class);

        // source data collection must exist
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSourceDataCollectionId(-99L)
        ))).isInstanceOf(EntityNotFoundException.class);

        // sink data collection must exist
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkDataCollectionId(-99L)
        ))).isInstanceOf(EntityNotFoundException.class);

        // sink instance must exist when sink data system type in [MYSQL,TIDB]
        Assertions.assertThat(Assertions.catchThrowable(() -> createConnection(connectionDetailDTO, it -> it.setSinkInstanceId(-99L)
        ))).isInstanceOf(EntityNotFoundException.class);
    }

    @Test
    public void testBatchUpdate() {
        // mock
        List<ConnectionDetailDTO> fakeConnectionDetailDTOs = createConnectionDetailLimit2();
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurationDTOs = getConnectionColumnConfigurationLimit1();
        ConnectionDetailDTO toSaveConnectionDTO = fakeConnectionDetailDTOs.get(0);
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(Lists.newArrayList(toSaveConnectionDTO), DOMAIN_ACCOUNT_ADMIN).get(0);

        savedConnectionDetailDTO.setConnectionColumnConfigurations(connectionColumnConfigurationDTOs);
        connectionService.batchUpdate(Lists.newArrayList(savedConnectionDetailDTO));

        entityManager.flush();
        entityManager.clear();

        // execute
        ConnectionDetailDTO connectionDetailByGet = connectionService.getDetailById(savedConnectionDetailDTO.getId());

        // assert
        Assertions.assertThat(connectionDetailByGet.getVersion()).isEqualTo(1);

        for (ConnectionColumnConfigurationDTO columnConfiguration : connectionDetailByGet.getConnectionColumnConfigurations()) {
            Assertions.assertThat(columnConfiguration.getConnectionVersion()).isEqualTo(1);
        }

        entityManager.flush();
        entityManager.clear();

        ConnectionColumnConfigurationDTO expectConnectionColumnConfiguration = connectionColumnConfigurationDTOs.get(0);
        String sql = "select * from connection_column_configuration where connection_id=" + savedConnectionDetailDTO.getId();
        Object[] result = (Object[]) entityManager.createNativeQuery(sql).getSingleResult();

        Assertions.assertThat(result[3]).isEqualTo(expectConnectionColumnConfiguration.getSourceColumnName());
        Assertions.assertThat(result[4]).isEqualTo(expectConnectionColumnConfiguration.getSinkColumnName());
    }

    @Test
    public void testUpdateShouldThrowExceptionWhenIllegalParameter() {
        ConnectionDetailDTO connectionDetailDTO = createConnectionDetail();
        Assertions.assertThat(Assertions.catchThrowable(() -> connectionService
                        .batchCreate(Lists.newArrayList(), DOMAIN_ACCOUNT_ADMIN)))
                .isInstanceOf(ClientErrorException.class);

        // sink data collection id required
        Assertions.assertThat(Assertions.catchThrowable(() -> updateConnection(connectionDetailDTO, it -> it.setId(null)
        ))).isInstanceOf(ClientErrorException.class);

        // special configuration json format
        Assertions.assertThat(Assertions.catchThrowable(() -> updateConnection(connectionDetailDTO, it -> it.setSpecificConfiguration("666")
        ))).isInstanceOf(ClientErrorException.class);

        // column configurations
        Assertions.assertThat(Assertions.catchThrowable(() -> updateConnection(connectionDetailDTO, it -> it.setConnectionColumnConfigurations(null)
        ))).isInstanceOf(ClientErrorException.class);
    }

    @Test(expected = EntityNotFoundException.class)
    public void testUpdateShouldThrowExceptionWhenNotFound() {
        ConnectionDetailDTO connectionDetailDTO = createConnectionDetail();
        connectionDetailDTO.setId(-99L);
        connectionService.batchUpdate(Lists.newArrayList(connectionDetailDTO));
    }

    @Test
    public void testQuery() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        List<ConnectionDetailDTO> savedConnectionDetailDTOs = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN);
        ConnectionQuery connectionQuery = ConnectionQuery.builder()
                .sinkDataCollectionIds(savedConnectionDetailDTOs.stream().map(it -> it.getSinkDataCollectionId()).collect(Collectors.toSet()))
                .build();

        List<ConnectionDTO> connectionDTOs = connectionService.query(connectionQuery);

        Assertions.assertThat(connectionDTOs.size()).isEqualTo(2);
    }

    // TODO
    @Test
    public void testPagedQuery() {

    }

    @Test
    public void testGetById() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        ConnectionDTO obtainedConnectionDTO = connectionService.getById(savedConnectionDetailDTO.getId());
        Assertions.assertThat(obtainedConnectionDTO.getId()).isEqualTo(savedConnectionDetailDTO.getId());
    }

    @Test(expected = EntityNotFoundException.class)
    public void testGetByIdShouldThrowExceptionWhenNotFound() {
        connectionService.getById(-99L);
    }

    @Test
    public void testGetByIds() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        List<ConnectionDetailDTO> savedConnectionDetailDTOs = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN);

        Set<Long> savedConnectionIds = savedConnectionDetailDTOs
                .stream()
                .map(it -> it.getId())
                .collect(Collectors.toSet());

        Assertions.assertThat(connectionService.getByIds(savedConnectionIds).size()).isEqualTo(2);
    }

    @Test
    public void testGetByIdsShouldReturnEmptyWhenNotFoundAny() {
        Set<Long> savedConnectionIds = Sets.newHashSet(-92L, -91L);
        List<ConnectionDTO> connectionDTOs = connectionService.getByIds(savedConnectionIds);
        Assertions.assertThat(connectionDTOs).isEmpty();
    }

    @Test
    public void testGetDetailById() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        ConnectionDetailDTO obtainedConnectionDetailDTO = connectionService.getDetailById(savedConnectionDetailDTO.getId());
        Assertions.assertThat(obtainedConnectionDetailDTO.getId()).isEqualTo(savedConnectionDetailDTO.getId());
    }

    @Test(expected = EntityNotFoundException.class)
    public void testGetDetailByIdShouldThrowExceptionWhenNotFound() {
        connectionService.getDetailById(-99L);
    }

    @Test
    public void testDeleteById() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        connectionService.deleteById(savedConnectionDetailDTO.getId());
        Assertions.assertThat(connectionService.getById(savedConnectionDetailDTO.getId()).isDeleted())
                .isTrue();
        Assertions.assertThat(connectionService.getById(savedConnectionDetailDTO.getId()).getDesiredState())
                .isEqualTo(ConnectionState.STOPPED);
    }

    @Test(expected = EntityNotFoundException.class)
    public void testDeleteByIdShouldThrowExceptionWhenNotFound() {
        connectionService.deleteById(-99L);
    }

    @Test
    public void testUpdateActualState() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        connectionService.updateActualState(savedConnectionDetailDTO.getId(), ConnectionState.RUNNING);
        ConnectionDTO connectionDTOByGet = connectionService.getById(savedConnectionDetailDTO.getId());
        Assertions.assertThat(connectionDTOByGet.getActualState()).isEqualTo(ConnectionState.RUNNING);
    }

    @Test
    public void testUpdateDesiredState() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        connectionService.updateDesiredState(savedConnectionDetailDTO.getId(), ConnectionState.RUNNING);
        ConnectionDTO connectionDTOByGet = connectionService.getById(savedConnectionDetailDTO.getId());
        Assertions.assertThat(connectionDTOByGet.getDesiredState()).isEqualTo(ConnectionState.RUNNING);
    }

    @Test
    public void testGetActualState() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        ConnectionState actualState = connectionService.getActualState(savedConnectionDetailDTO.getId());
        Assertions.assertThat(actualState).isEqualTo(ConnectionState.STOPPED);
    }

    @Test
    public void testUpdateConnectionRequisitionStateByQuery() {
        List<ConnectionDetailDTO> toSaveConnectionDetailDTOs = createConnectionDetailLimit2();
        ConnectionDetailDTO savedConnectionDetailDTO = connectionService.batchCreate(toSaveConnectionDetailDTOs, DOMAIN_ACCOUNT_ADMIN).get(0);
        ConnectionQuery connectionQuery = ConnectionQuery.builder()
                .connectionIds(Arrays.asList(savedConnectionDetailDTO.getId()))
                .build();
        connectionService.updateConnectionRequisitionStateByQuery(connectionQuery, RequisitionState.APPROVED);

        List<ConnectionDTO> connectionDTOs = connectionService.query(ConnectionQuery.builder().build());
        Assertions.assertThat(connectionDTOs.size()).isEqualTo(2);
        for (ConnectionDTO connectionDTO : connectionDTOs) {
            if (Objects.equals(connectionDTO.getId(), savedConnectionDetailDTO.getId())) {
                Assertions.assertThat(connectionDTO.getRequisitionState()).isEqualTo(RequisitionState.APPROVED);
            }
        }
    }

    @Test
    public void testCreateOrUpdateSourceConnectorShouldCreateConnectorWhenInitialCreation() {
        // mock
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();
        ConnectionDTO connectionDTO = createConnection();
        ConnectorClassDetailDTO connectorClassDetailDTO = createConnectorClassDetailOfMySqlConnectorClass();
        KafkaClusterDTO acdcKafkaClusterDTO = createACDCKafkaCluster();

        // data system
        when(mockDataSystemResourceService.getDataSystemType(anyLong())).thenReturn(DataSystemType.MYSQL);
        when(mockDataSystemResourceService.getById(any())).thenReturn(createMysqlTableDataSystemResource());
        when(mockDataSystemResourceService.getParent(any(), any())).thenReturn(createMysqlDatabaseDataSystemResource());

        // data system source connector service
        when(mockDataSystemServiceManager.getDataSystemSourceConnectorService(any())).thenReturn(mockDataSystemSourceConnectorService);
        when(mockDataSystemSourceConnectorService.getConnectorDataSystemResourceType()).thenReturn(DataSystemResourceType.MYSQL_DATABASE);
        when(mockDataSystemSourceConnectorService.generateKafkaTopicName(anyLong())).thenReturn("mysql-1-db-tb");
        when(mockDataSystemSourceConnectorService.generateConnectorName(anyLong())).thenReturn("source-mysql-1-db");
        when(mockDataSystemSourceConnectorService.getConnectorDefaultConfiguration()).thenReturn(new HashMap<>());
        when(mockDataSystemSourceConnectorService.generateConnectorCustomConfiguration(any())).thenReturn(new HashMap<>());
        when(mockDataSystemSourceConnectorService.getConnectorClass()).thenReturn(connectorClassDetailDTO);

        // kafka topic and kafka cluster
        when(mockKafkaClusterService.getACDCKafkaCluster()).thenReturn(acdcKafkaClusterDTO);
//        when(mockKafkaTopicService.createKafkaTopicInACDCAndKafkaForDataSystemResource(any(), any(), any())).thenReturn(kafkaTopicDetailDTO);
        when(mockDataSystemResourceRepository.findById(any())).thenReturn(Optional.of(new DataSystemResourceDO()));

        // connector
        when(mockConnectorService.getDetailByDataSystemResourceId(any())).thenReturn(Optional.empty());

        connectionService.createOrUpdateSourceConnector(connectionDTO);

        // verify
        ArgumentCaptor<Long> sourceDataCollectionIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> kafkaClusterIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<String> kafkaTopicNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ConnectorDetailDTO> connectorDetailDTOCaptor = ArgumentCaptor.forClass(ConnectorDetailDTO.class);
        ArgumentCaptor<List<Long>> needSubscribedDataCollectionIdsCaptor = ArgumentCaptor.forClass(List.class);

        Mockito.verify(mockConnectorService, Mockito.times(0)).updateParticularConfiguration(any(), any());

        Mockito.verify(mockDataSystemResourceService, Mockito.times(1)).getDataSystemType(sourceDataCollectionIdCaptor.capture());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockDataSystemResourceService, Mockito.times(1)).getById(sourceDataCollectionIdCaptor.capture());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockDataSystemResourceService, Mockito.times(1)).getParent(sourceDataCollectionIdCaptor.capture(), any());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).verifyDataSystemMetadata(sourceDataCollectionIdCaptor.capture());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).generateKafkaTopicName(sourceDataCollectionIdCaptor.capture());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).generateConnectorCustomConfiguration(needSubscribedDataCollectionIdsCaptor.capture());
        Assertions.assertThat(needSubscribedDataCollectionIdsCaptor.getValue()).contains(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockKafkaTopicService, Mockito.times(1)).createDataCollectionTopicIfAbsent(
                sourceDataCollectionIdCaptor.capture(),
                kafkaClusterIdCaptor.capture(),
                kafkaTopicNameCaptor.capture()
        );

        sourceDataCollectionIdCaptor.getAllValues().forEach(each -> Assertions.assertThat(each).isEqualTo(connectionDTO.getSourceDataCollectionId()));

        Assertions.assertThat(kafkaClusterIdCaptor.getValue()).isEqualTo(acdcKafkaClusterDTO.getId());
        Assertions.assertThat(kafkaTopicNameCaptor.getValue()).isEqualTo("mysql-1-db-tb");

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).beforeConnectorCreation(sourceDataCollectionIdCaptor.capture());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).generateConnectorName(sourceDataCollectionIdCaptor.capture());
        Assertions.assertThat(sourceDataCollectionIdCaptor.getValue()).isEqualTo(connectionDTO.getSourceDataCollectionId());

        Mockito.verify(mockConnectorService, Mockito.times(1)).create(connectorDetailDTOCaptor.capture());

        Assertions.assertThat(connectorDetailDTOCaptor.getValue().getActualState()).isEqualTo(ConnectorState.PENDING);
        Assertions.assertThat(connectorDetailDTOCaptor.getValue().getDesiredState()).isEqualTo(ConnectorState.RUNNING);
        Assertions.assertThat(connectorDetailDTOCaptor.getValue().getName()).isEqualTo("source-mysql-1-db");
        Assertions.assertThat(connectorDetailDTOCaptor.getValue().getConnectClusterId()).isEqualTo(connectorClassDetailDTO.getConnectClusters().get(0).getId());
        Assertions.assertThat(connectorDetailDTOCaptor.getValue().getKafkaClusterId()).isEqualTo(acdcKafkaClusterDTO.getId());
    }

    @Test
    public void testCreateOrUpdateSourceConnectorShouldUpdateConnectorConfigurationWhenAlreadyExistConnector() {
        // mock
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();
        ConnectionDTO connectionDTO = createConnection();
        KafkaClusterDTO acdcKafkaClusterDTO = createACDCKafkaCluster();
        List<ConnectionDO> connectionDOs = createConnectionDOLimit2();

        when(mockConnectionRepository.query(any())).thenReturn(createConnectionDOLimit2());

        // data system
        when(mockDataSystemResourceService.getDataSystemType(anyLong())).thenReturn(DataSystemType.MYSQL);
        when(mockDataSystemResourceService.getById(any())).thenReturn(createMysqlTableDataSystemResource());
        when(mockDataSystemResourceService.getParent(any(), any())).thenReturn(createMysqlDatabaseDataSystemResource());

        // data system source connector service
        when(mockDataSystemServiceManager.getDataSystemSourceConnectorService(any())).thenReturn(mockDataSystemSourceConnectorService);
        when(mockDataSystemSourceConnectorService.getConnectorDataSystemResourceType()).thenReturn(DataSystemResourceType.MYSQL_DATABASE);
        when(mockDataSystemSourceConnectorService.generateConnectorCustomConfiguration(any())).thenReturn(new HashMap<>());

        // kafka topic and kafka cluster
        when(mockKafkaClusterService.getACDCKafkaCluster()).thenReturn(acdcKafkaClusterDTO);
//        when(mockKafkaTopicService.getKafkaTopicByDataSystemResourceId(any())).thenReturn(kafkaTopicDTO);
        when(mockDataSystemResourceRepository.findById(any())).thenReturn(Optional.of(new DataSystemResourceDO()));

        // connector
        when(mockConnectorService.getDetailByDataSystemResourceId(any())).thenReturn(Optional.of(createSourceConnector("source-mysql-1-db")));
        when(mockConnectorService.getDetailById(any())).thenReturn(createSourceConnectorDetail("source-1-db1", 2L));

        connectionService.createOrUpdateSourceConnector(connectionDTO);

        ArgumentCaptor<List<Long>> needSubscribedDataCollectionIdsCaptor = ArgumentCaptor.forClass(List.class);

        Mockito.verify(mockConnectorService, Mockito.times(1)).updateParticularConfiguration(any(), any());

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).generateConnectorCustomConfiguration(needSubscribedDataCollectionIdsCaptor.capture());
        Assertions.assertThat(needSubscribedDataCollectionIdsCaptor.getValue().size()).isEqualTo(connectionDOs.size() + 1);

        Mockito.verify(mockDataSystemSourceConnectorService, Mockito.times(1)).getImmutableConfigurationNames();
        Mockito.verify(mockConnectorService, Mockito.times(2)).getDetailById(any());
    }

    @Test
    public void testDiscardImmutableConnectorConfiguration() {
        // mock
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        long connectorId = 2L;

        Map<String, String> newConnectorConfiguration = new HashMap<>();
        newConnectorConfiguration.put("database.history.kafka.topic", "schema_history-server1");
        newConnectorConfiguration.put("database.server.name", "server1");
        newConnectorConfiguration.put("k3", "v3");

        Set<String> immutableConnectorConfigurationNames = Sets.newHashSet(
                "database.history.kafka.topic",
                "database.server.name"
        );

        Map<String, String> expectConfigurationMap = new HashMap<>();
        expectConfigurationMap.put("k3", "v3");

        when(mockConnectorService.getDetailById(eq(connectorId))).thenReturn(createSourceConnectorDetail("source-1-db1", connectorId));

        connectionService.discardImmutableConnectorConfiguration(connectorId, newConnectorConfiguration, immutableConnectorConfigurationNames);

        Assertions.assertThat(newConnectorConfiguration.size()).isEqualTo(1);
        Assertions.assertThat(newConnectorConfiguration).isEqualTo(expectConfigurationMap);
    }

    @Test
    public void testApplyConnectionConfigurationToConnector() {
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        ConnectionDO connectionDO = createConnectionDOWithSinkConnector();
        ConnectorDetailDTO sinkConnectorDetail = createSinkConnectorDetail("sink-mysql-city", connectionDO.getSinkConnector().getId());
        Map<String, String> customConfiguration = Maps.newHashMap("k1", "v1");

        // mock
        when(mockDataSystemServiceManager.getDataSystemSinkConnectorService(connectionDO.getSinkDataSystemType())).thenReturn(mockDataSystemSinkConnectorService);
        when(mockDataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionDO.getId())).thenReturn(customConfiguration);
        when(mockConnectionRepository.getOne(connectionDO.getId())).thenReturn(connectionDO);
        when(mockConnectorService.getDetailById(sinkConnectorDetail.getId())).thenReturn(sinkConnectorDetail);
        when(mockDataSystemServiceManager.getDataSystemSinkConnectorService(connectionDO.getSinkDataSystemType())).thenReturn(mockDataSystemSinkConnectorService);

        connectionService.applyConnectionConfigurationToConnector(connectionDO.getId());

        // verify
        ArgumentCaptor<DataSystemType> dataSystemCapture = ArgumentCaptor.forClass(DataSystemType.class);
        Mockito.verify(mockDataSystemServiceManager, Mockito.times(1)).getDataSystemSinkConnectorService(dataSystemCapture.capture());
        Assertions.assertThat(dataSystemCapture.getValue()).isEqualTo(connectionDO.getSinkDataSystemType());

        ArgumentCaptor<Long> connectionIdCapture = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockDataSystemSinkConnectorService, Mockito.times(1)).generateConnectorCustomConfiguration(connectionIdCapture.capture());
        Assertions.assertThat(connectionIdCapture.getValue()).isEqualTo(connectionDO.getId());

        ArgumentCaptor<Long> connectorIdCapture = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Map<String, String>> customConfigurationCapture = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(mockConnectorService, Mockito.times(1)).updateParticularConfiguration(connectorIdCapture.capture(), customConfigurationCapture.capture());
        Assertions.assertThat(connectorIdCapture.getValue()).isEqualTo(sinkConnectorDetail.getId());
        Assertions.assertThat(customConfigurationCapture.getValue()).isEqualTo(customConfiguration);
    }

    @Test
    public void testApplyConnectionConfigurationToConnectorShouldDoNothingWhenSinkConnectorNotExistInConnection() {
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        ConnectionDO connectionDO = createConnectionDOWithSinkConnector();

        // not set sink connector
        connectionDO.setSinkConnector(null);

        // mock
        when(mockConnectionRepository.getOne(connectionDO.getId())).thenReturn(connectionDO);

        connectionService.applyConnectionConfigurationToConnector(connectionDO.getId());

        // verify
        Mockito.verify(mockDataSystemServiceManager, Mockito.times(0)).getDataSystemSinkConnectorService(any());
    }

    @Test
    public void testCreateResourceKafkaTopicIfAbsent() {
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        KafkaClusterDTO acdcKafkaClusterDTO = createACDCKafkaCluster();
        DataSystemResourceDTO dataSystemResourceDTO = new DataSystemResourceDTO().setId(1L).setKafkaTopicId(2L);

        when(mockDataSystemResourceService.getById(any())).thenReturn(dataSystemResourceDTO);
        when(mockKafkaClusterService.getACDCKafkaCluster()).thenReturn(acdcKafkaClusterDTO);

        connectionService.createResourceKafkaTopicIfAbsent(dataSystemResourceDTO.getId(), "test_topic");

        Mockito.verify(mockKafkaTopicService, Mockito.times(1))
                .createDataCollectionTopicIfAbsent(eq(dataSystemResourceDTO.getId()), eq(acdcKafkaClusterDTO.getId()), eq("test_topic"));
    }

    @Test
    public void testGetSourceConnectorDataSystemResourceAlreadyAppliedConnectionsShouldGetEmptyWhenConnectorNotExist() {
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        when(mockConnectorService.getDetailByDataSystemResourceId(any())).thenReturn(Optional.empty());

        List<ConnectionDO> connectionDOs = connectionService.getSourceConnectorDataSystemResourceAlreadyAppliedConnections(1L);

        Assertions.assertThat(connectionDOs).isEmpty();
    }

    @Test
    public void testGetSourceConnectorDataSystemResourceAlreadyAppliedConnectionsShouldGetNotEmptyListWhenConnectorExist() {
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        when(mockConnectorService.getDetailByDataSystemResourceId(any())).thenReturn(Optional.of(createSourceConnector("source-mysql-1-db")));
        when(mockConnectionRepository.query(any())).thenReturn(createConnectionDOLimit2());

        List<ConnectionDO> connectionDOs = connectionService.getSourceConnectorDataSystemResourceAlreadyAppliedConnections(1L);

        Assertions.assertThat(connectionDOs).isNotEmpty();
    }

    @Test
    public void testGetSourceConnectorNeedSubscribedDataCollectionIdsShouldRemoveDuplication() {
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();

        List<ConnectionDO> connectionDOs = createConnectionDOLimit2();

        long sourceDataCollectionId = connectionDOs.get(0).getSourceDataCollection().getId();

        List<Long> ids = connectionService.getSourceConnectorNeedSubscribedDataCollectionIds(sourceDataCollectionId, connectionDOs);

        Assertions.assertThat(ids.size()).isEqualTo(connectionDOs.size());
    }

    @Test
    public void testCreateSinkConnector() {
        // mock
        ConnectionServiceImpl connectionService = (ConnectionServiceImpl) newConnectionServiceWithMockDependence();
        ConnectionDTO connectionDTO = createConnection();
        KafkaClusterDTO acdcKafkaClusterDTO = createACDCKafkaCluster();
        ConnectorClassDetailDTO connectorClassDetailDTO = createConnectorClassDetailOfMySqlConnectorClass();

        when(mockConnectionRepository.query(any())).thenReturn(createConnectionDOLimit2());

        // data system
        when(mockDataSystemResourceService.getDataSystemType(anyLong())).thenReturn(DataSystemType.MYSQL);
        when(mockDataSystemResourceService.getById(any())).thenReturn(createMysqlTableDataSystemResource());
        when(mockDataSystemResourceService.getParent(any(), any())).thenReturn(createMysqlDatabaseDataSystemResource());

        // data system sink connector service
        when(mockDataSystemServiceManager.getDataSystemSinkConnectorService(any())).thenReturn(mockDataSystemSinkConnectorService);
        when(mockDataSystemSinkConnectorService.generateConnectorCustomConfiguration(any())).thenReturn(new HashMap<>());
        when(mockDataSystemSinkConnectorService.getConnectorClass()).thenReturn(connectorClassDetailDTO);

        // kafka topic and kafka cluster
        when(mockKafkaClusterService.getACDCKafkaCluster()).thenReturn(acdcKafkaClusterDTO);

        connectionService.createSinkConnector(connectionDTO);

        Mockito.verify(mockDataSystemSinkConnectorService, Mockito.times(1)).verifyDataSystemMetadata(eq(connectionDTO.getSinkDataCollectionId()));
        Mockito.verify(mockDataSystemSinkConnectorService, Mockito.times(1)).beforeConnectorCreation(eq(connectionDTO.getSinkDataCollectionId()));
        Mockito.verify(mockDataSystemSinkConnectorService, Mockito.times(1)).generateConnectorName(eq(connectionDTO.getId()));
    }

    private void createAuthorityRoles() {
        Arrays.stream(AuthorityRoleType.values()).forEach(each -> {
            authorityRepository.save(new AuthorityDO().setName(each.name()));
        });
    }

    private UserDO createAdminUser() {
        UserDO user = UserDO.builder()
                .name(DOMAIN_ACCOUNT_ADMIN)
                .email(DOMAIN_ACCOUNT_ADMIN + "@test.cn")
                .domainAccount(DOMAIN_ACCOUNT_ADMIN)
                .password("456")
                .createdBy(SystemConstant.ACDC)
                .updatedBy(SystemConstant.ACDC)
                .authorities(Sets.newHashSet(
                        AuthorityDO.builder().name(AuthorityRoleType.ROLE_ADMIN.name()).build()
                ))
                .build();

        return userRepository.save(user);
    }

    private UserDO createNormalUser() {
        UserDO user = UserDO.builder()
                .name(DOMAIN_ACCOUNT_NORMAL)
                .email(DOMAIN_ACCOUNT_NORMAL + "@test.cn")
                .domainAccount(DOMAIN_ACCOUNT_NORMAL)
                .password("456")
                .createdBy(SystemConstant.ACDC)
                .updatedBy(SystemConstant.ACDC)
                .authorities(Sets.newHashSet(
                        AuthorityDO.builder().name(AuthorityRoleType.ROLE_USER.name()).build()
                ))
                .build();

        return userRepository.save(user);
    }

    private List<ConnectionDetailDTO> createConnectionDetailLimit2() {
        // save mysql cluster resource
        DataSystemResourceDO mysqlCluster = saveDataSystemResource("cluster1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_CLUSTER, null);

        // save mysql database resource
        DataSystemResourceDO mysqlDataBase = saveDataSystemResource("db1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_DATABASE, mysqlCluster);

        // save sink mysql instance resource
        DataSystemResourceDO mysqlInstance = saveDataSystemResource("instance1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_INSTANCE, mysqlCluster);

        // save data collection resource
        DataSystemResourceDO sourceTb1 = saveDataSystemResource("source_tb1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, mysqlDataBase);
        DataSystemResourceDO sourceTb2 = saveDataSystemResource("source_tb2", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, mysqlDataBase);
        DataSystemResourceDO sinkTb1 = saveDataSystemResource("sink_tb1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, mysqlDataBase);
        DataSystemResourceDO sinkTb2 = saveDataSystemResource("sink_tb2", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, mysqlDataBase);

        // project
        ProjectDO sourcePrj = saveProject("source_prj", DOMAIN_ACCOUNT_ADMIN);
        ProjectDO sinkPrj = saveProject("sink_prj", DOMAIN_ACCOUNT_ADMIN);

        return Lists.newArrayList(
                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.MYSQL)
                        .sourceProjectId(sourcePrj.getId())
                        .sourceDataCollectionId(sourceTb1.getId())

                        .sinkProjectId(sinkPrj.getId())
                        .sinkDataCollectionId(sinkTb1.getId())
                        .sinkInstanceId(mysqlInstance.getId())
                        .sinkDataSystemType(DataSystemType.MYSQL)

                        .connectionColumnConfigurations(Lists.newArrayList(
                                columnConOf("a1", "bigint(20)", "PRIMARY", "b1", "bigint(50)", "PRIMARY", null, null),
                                columnConOf("a2", "bigint(20)", null, "b2", "bigint(50)", null, ">", "10"),
                                columnConOf("a3", "bigint(20)", null, "b3", "bigint(50)", null, "<", "20")
                        ))
                        .build(),

                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.MYSQL)
                        .sourceProjectId(sourcePrj.getId())
                        .sourceDataCollectionId(sourceTb2.getId())

                        .sinkProjectId(sinkPrj.getId())
                        .sinkDataCollectionId(sinkTb2.getId())
                        .sinkInstanceId(mysqlInstance.getId())
                        .sinkDataSystemType(DataSystemType.MYSQL)
                        .connectionColumnConfigurations(Lists.newArrayList(
                                columnConOf("c1", "bigint(20)", "PRIMARY", "d1", "bigint(50)", "PRIMARY", null, null),
                                columnConOf("c2", "bigint(20)", null, "d2", "bigint(50)", null, ">=", "10"),
                                columnConOf("c3", "bigint(20)", null, "d3", "bigint(50)", null, "<=", "20")
                        ))
                        .build()
        );
    }

    private List<ConnectionDetailDTO> createSameConnectionDetailLimit2() {
        // save mysql database resource
        DataSystemResourceDO mysqlDataBase = saveDataSystemResource("db1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_DATABASE, null);

        // save mysql cluster resource
        DataSystemResourceDO mysqlCluster = saveDataSystemResource("cluster1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_CLUSTER, null);

        // save sink mysql instance resource
        DataSystemResourceDO mysqlInstance = saveDataSystemResource("instance1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_INSTANCE, mysqlCluster);

        // save data collection resource
        DataSystemResourceDO sourceTb1 = saveDataSystemResource("source_tb1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, mysqlDataBase);
        DataSystemResourceDO sinkTb1 = saveDataSystemResource("sink_tb1", DataSystemType.MYSQL, DataSystemResourceType.MYSQL_TABLE, mysqlDataBase);

        // project
        ProjectDO sourcePrj = saveProject("source_prj", DOMAIN_ACCOUNT_ADMIN);
        ProjectDO sinkPrj = saveProject("sink_prj", DOMAIN_ACCOUNT_ADMIN);

        return Lists.newArrayList(
                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.MYSQL)
                        .sourceProjectId(sourcePrj.getId())
                        .sourceDataCollectionId(sourceTb1.getId())

                        .sinkProjectId(sinkPrj.getId())
                        .sinkDataCollectionId(sinkTb1.getId())
                        .sinkInstanceId(mysqlInstance.getId())
                        .sinkDataSystemType(DataSystemType.MYSQL)

                        .connectionColumnConfigurations(Lists.newArrayList(
                                columnConOf("a1", "bigint(20)", "PRIMARY", "b1", "bigint(50)", "PRIMARY", null, null),
                                columnConOf("a2", "bigint(20)", null, "b2", "bigint(50)", null, ">", "10"),
                                columnConOf("a3", "bigint(20)", null, "b3", "bigint(50)", null, "<", "20")
                        ))
                        .build(),

                ConnectionDetailDTO.builder()
                        .sourceDataSystemType(DataSystemType.MYSQL)
                        .sourceProjectId(sourcePrj.getId())
                        .sourceDataCollectionId(sourceTb1.getId())

                        .sinkProjectId(sinkPrj.getId())
                        .sinkDataCollectionId(sinkTb1.getId())
                        .sinkInstanceId(mysqlInstance.getId())
                        .sinkDataSystemType(DataSystemType.MYSQL)
                        .connectionColumnConfigurations(Lists.newArrayList(
                                columnConOf("c1", "bigint(20)", "PRIMARY", "d1", "bigint(50)", "PRIMARY", null, null),
                                columnConOf("c2", "bigint(20)", null, "d2", "bigint(50)", null, ">=", "10"),
                                columnConOf("c3", "bigint(20)", null, "d3", "bigint(50)", null, "<=", "20")
                        ))
                        .build()
        );
    }

    private List<ConnectionColumnConfigurationDTO> getConnectionColumnConfigurationLimit1() {
        return Lists.newArrayList(
                columnConOf("new_source_column_name", "bigint(20)", "PRIMARY", "new_sink_column_name", "bigint(50)", "PRIMARY", null, null)
        );
    }

    private ConnectionDetailDTO createConnectionDetail() {
        return ConnectionDetailDTO.builder()
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sourceProjectId(1L)
                .sourceDataCollectionId(1L)

                .sinkProjectId(2L)
                .sinkDataCollectionId(2L)
                .sinkInstanceId(2L)
                .sinkDataSystemType(DataSystemType.MYSQL)
                .connectionColumnConfigurations(Lists.newArrayList(
                        columnConOf("c1", "bigint(20)", "PRIMARY", "d1", "bigint(50)", "PRIMARY", null, null),
                        columnConOf("c2", "bigint(20)", null, "d2", "bigint(50)", null, ">=", "10"),
                        columnConOf("c3", "bigint(20)", null, "d3", "bigint(50)", null, "<=", "20")
                ))
                .build();
    }

    private DataSystemResourceDO saveDataSystemResource(
            final String name,
            final DataSystemType dataSystemType,
            final DataSystemResourceType dataSystemResourceType,
            final DataSystemResourceDO parentResource

    ) {
        return dataSystemResourceRepository.save(
                DataSystemResourceDO.builder()
                        .name(name)
                        .dataSystemType(dataSystemType)
                        .resourceType(dataSystemResourceType)
                        .parentResource(parentResource)
                        .build()
        );
    }

    private ProjectDO saveProject(final String name, final String ownerDomainAccount) {
        ProjectDO projectDO = ProjectDO.builder()
                .name(name)
                .owner(userRepository.findOneByDomainAccountIgnoreCase(ownerDomainAccount).get())
                .build();
        return projectRepository.save(projectDO);
    }

    private ProjectDO saveProjectWithoutOwner(final String name) {
        ProjectDO projectDO = ProjectDO.builder()
                .name(name)
                .build();
        return projectRepository.save(projectDO);
    }

    private ConnectionColumnConfigurationDTO columnConOf(
            final String sourceColumnName,
            final String sourceColumnType,
            final String sourceColumnUniqueIndexNames,
            final String sinkColumnName,
            final String sinkColumnType,
            final String sinkColumnUniqueIndexNames,
            final String filterOperator,
            final String filterValue

    ) {
        return new ConnectionColumnConfigurationDTO()
                .setSourceColumnName(sourceColumnName)
                .setSourceColumnType(sourceColumnType)
                .setSourceColumnUniqueIndexNames(StringUtil.convertStringToSetWithSeparator(sourceColumnUniqueIndexNames, Symbol.COMMA))

                .setSinkColumnName(sinkColumnName)
                .setSinkColumnType(sinkColumnType)
                .setSinkColumnUniqueIndexNames(StringUtil.convertStringToSetWithSeparator(sinkColumnUniqueIndexNames, Symbol.COMMA))
                .setFilterOperator(filterOperator)
                .setFilterValue(filterValue);
    }

    private void verifySavedConnection(final ConnectionDetailDTO connectionDetailDTO, final Integer expectVersion, final boolean isNeedVerifySinkInstance) {
        Assertions.assertThat(connectionDetailDTO.getVersion()).isEqualTo(expectVersion);

        Assertions.assertThat(connectionDetailDTO.getSourceProjectId()).isNotNull();
        Assertions.assertThat(connectionDetailDTO.getSourceDataSystemType()).isNotNull();
        Assertions.assertThat(connectionDetailDTO.getSourceDataCollectionId()).isNotNull();

        Assertions.assertThat(connectionDetailDTO.getSinkProjectId()).isNotNull();
        Assertions.assertThat(connectionDetailDTO.getSinkDataSystemType()).isNotNull();
        Assertions.assertThat(connectionDetailDTO.getSinkDataCollectionId()).isNotNull();
        Assertions.assertThat(connectionDetailDTO.getSinkDataCollectionId()).isNotNull();
        Assertions.assertThat(connectionDetailDTO.getRequisitionState()).isEqualTo(RequisitionState.APPROVING);
        Assertions.assertThat(connectionDetailDTO.getDesiredState()).isEqualTo(ConnectionState.STOPPED);
        Assertions.assertThat(connectionDetailDTO.getActualState()).isEqualTo(ConnectionState.STOPPED);

        if (isNeedVerifySinkInstance) {
            Assertions.assertThat(connectionDetailDTO.getSinkInstanceId()).isNotNull();
        }
    }

    private void updateConnection(
            final ConnectionDetailDTO connectionDetail,
            final Consumer<ConnectionDetailDTO> prepareFun) {
        prepareFun.accept(connectionDetail);
        connectionService.batchUpdate(Lists.newArrayList(connectionDetail));
    }

    private DataSystemResourceDTO createMysqlTableDataSystemResource() {
        return DataSystemResourceDTO.builder()
                .id(1L)
                .resourceType(DataSystemResourceType.MYSQL_TABLE)
                .build();
    }

    private DataSystemResourceDTO createMysqlDatabaseDataSystemResource() {
        return DataSystemResourceDTO.builder()
                .id(2L)
                .resourceType(DataSystemResourceType.MYSQL_DATABASE)
                .build();
    }

    private ConnectorClassDetailDTO createConnectorClassDetailOfMySqlConnectorClass() {
        List<ConnectClusterDTO> connectClusterDTOs = Lists.newArrayList(
                ConnectClusterDTO.builder()
                        .connectorClassId(1L)
                        .connectRestApiUrl("localhost:8083")
                        .build(),
                ConnectClusterDTO.builder()
                        .connectorClassId(2L)
                        .connectRestApiUrl("localhost:8084")
                        .build()
        );

        return ConnectorClassDetailDTO.builder()
                .connectorType(ConnectorType.SOURCE)
                .name("io.debezium.connector.mysql.MySqlConnector")
                .connectClusters(connectClusterDTOs)
                .dataSystemType(DataSystemType.MYSQL)
                .build();
    }

    private KafkaTopicDTO createKafkaTopic(final String name) {
        return KafkaTopicDTO.builder()
                .name(name)
                .id(1L)
                .build();
    }

    private KafkaTopicDetailDTO createKafkaTopicDetail(final String name) {
        return KafkaTopicDetailDTO.builder()
                .name(name)
                .id(1L)
                .build();
    }

    private KafkaClusterDTO createACDCKafkaCluster() {
        return KafkaClusterDTO.builder()
                .id(1L)
                .name("acdc-kafka-cluster")
                .clusterType(KafkaClusterType.INNER)
                .bootstrapServers("localhost:9092")
                .build();
    }

    private ConnectionDTO createConnection() {
        return ConnectionDTO.builder()
                .id(1L)
                .sourceProjectId(2L)
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sourceDataCollectionId(Long.MAX_VALUE - 1)
                .sinkProjectId(4L)
                .sinkDataSystemType(DataSystemType.MYSQL)
                .sinkInstanceId(5L)
                .sinkDataCollectionId(6L)
                .build();
    }

    private void createConnection(final ConnectionDetailDTO connectionDetail, final Consumer<ConnectionDetailDTO> prepareFun) {
        prepareFun.accept(connectionDetail);
        connectionService.batchCreate(Lists.newArrayList(connectionDetail), DOMAIN_ACCOUNT_ADMIN);
    }

    private ConnectionDO createConnectionDOWithSinkConnector() {
        return ConnectionDO.builder()
                .id(1L)
                .sourceProject(new ProjectDO(1L))
                .sourceDataSystemType(DataSystemType.MYSQL)
                .sourceDataCollection(new DataSystemResourceDO(1L))
                .sinkProject(new ProjectDO(2L))
                .sinkDataSystemType(DataSystemType.MYSQL)
                .sinkInstance(new DataSystemResourceDO(2L))
                .sinkDataCollection(new DataSystemResourceDO(2L))
                .sinkConnector(new ConnectorDO(2L))
                .user(new UserDO(1L))
                .build();
    }

    private List<ConnectionDO> createConnectionDOLimit2() {

        ConnectorClassDO connectorClassDO = ConnectorClassDO.builder()
                .id(1L)
                .name("io.debezium.connector.mysql.MySqlConnector")
                .connectorType(ConnectorType.SOURCE)
                .dataSystemType(DataSystemType.MYSQL)
                .defaultConnectorConfigurations(new HashSet<>())
                .connectClusters(new HashSet<>())
                .build();
        ConnectorDO connectorDO = ConnectorDO.builder()
                .id(2L)
                .name("source01")
                .actualState(ConnectorState.PENDING)
                .desiredState(ConnectorState.RUNNING)
                .connectCluster(new ConnectClusterDO(1L))
                .connectorClass(connectorClassDO)
                .kafkaCluster(new KafkaClusterDO(1L))
                .connectorConfigurations(new HashSet<>())
                .dataSystemResource(new DataSystemResourceDO(1L))
                .build();
        return Lists.newArrayList(
                ConnectionDO.builder().id(1L)
                        .sourceConnector(connectorDO)
                        .sourceDataCollection(new DataSystemResourceDO(3L))
                        .build(),
                ConnectionDO.builder().id(2L)
                        .sourceConnector(connectorDO)
                        .sourceDataCollection(new DataSystemResourceDO(4L))
                        .build()
        );
    }

    private ConnectorDetailDTO createSourceConnector(final String name) {
        return ConnectorDetailDTO.builder()
                .name(name)
                .id(1L)
                .actualState(ConnectorState.PENDING)
                .desiredState(ConnectorState.RUNNING)
                .connectorType(ConnectorType.SOURCE)
                .build();
    }

    private ConnectorDetailDTO createSinkConnectorDetail(final String name, final Long id) {
        return ConnectorDetailDTO.builder()
                .name(name)
                .id(id)
                .actualState(ConnectorState.PENDING)
                .desiredState(ConnectorState.RUNNING)
                .connectorType(ConnectorType.SINK)
                .build();
    }

    private ConnectorDetailDTO createSourceConnectorDetail(final String name, final Long id) {

        List<ConnectorConfigurationDTO> configurationDTOs = Lists.newArrayList(
                new ConnectorConfigurationDTO().setName("database.history.kafka.topic").setValue("schema_history-server1"),
                new ConnectorConfigurationDTO().setName("database.server.name").setValue("server1"),
                new ConnectorConfigurationDTO().setName("database.history.kafka.topic").setValue("topic1")
        );
        return ConnectorDetailDTO.builder()
                .name(name)
                .id(id)
                .actualState(ConnectorState.PENDING)
                .desiredState(ConnectorState.RUNNING)
                .connectorType(ConnectorType.SINK)
                .connectorConfigurations(configurationDTOs)
                .build();
    }

    private ConnectionService newConnectionServiceWithMockDependence() {
        ConnectionService connectionService = new ConnectionServiceImpl();
        ReflectionTestUtils.setField(connectionService, "connectionRepository", mockConnectionRepository);
        ReflectionTestUtils.setField(connectionService, "dataSystemResourceService", mockDataSystemResourceService);
        ReflectionTestUtils.setField(connectionService, "dataSystemServiceManager", mockDataSystemServiceManager);
        ReflectionTestUtils.setField(connectionService, "connectorService", mockConnectorService);
        ReflectionTestUtils.setField(connectionService, "kafkaClusterService", mockKafkaClusterService);
        ReflectionTestUtils.setField(connectionService, "kafkaTopicService", mockKafkaTopicService);

        return connectionService;
    }
}
