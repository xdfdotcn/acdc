package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DefaultConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class TidbDataSystemSinkConnectorServiceImplTest {
    
    @Autowired
    @Qualifier("tidbDataSystemSinkConnectorServiceImpl")
    private DataSystemSinkConnectorService dataSystemSinkConnectorService;
    
    @MockBean
    private ConnectorClassService connectorClassService;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private ConnectionService connectionService;
    
    @Before
    public void setUp() throws Exception {
    
    }
    
    @Test
    public void testGetConnectorDefaultConfigurationShouldAsExpect() {
        Mockito.when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(
                        ArgumentMatchers.eq(DataSystemType.TIDB), ArgumentMatchers.eq(ConnectorType.SINK)))
                .thenReturn(fakeConnectorClassDetailDTO());
        
        Map<String, String> expected = fakeConnectorClassDetailDTO().getDefaultConnectorConfigurations().stream()
                .collect(Collectors.toMap(DefaultConnectorConfigurationDTO::getName, DefaultConnectorConfigurationDTO::getValue));
        
        Assertions.assertThat(dataSystemSinkConnectorService.getConnectorDefaultConfiguration())
                .isEqualTo(expected);
    }
    
    private ConnectorClassDetailDTO fakeConnectorClassDetailDTO() {
        Set<DefaultConnectorConfigurationDTO> configs = new HashSet<>();
        configs.add(new DefaultConnectorConfigurationDTO().setName("config_1").setValue("value_1"));
        configs.add(new DefaultConnectorConfigurationDTO().setName("config_2").setValue("value_2"));
        return new ConnectorClassDetailDTO().setDefaultConnectorConfigurations(configs);
    }
    
    @Test
    public void testGenerateConnectorCustomConfigurationShouldAsExpect() {
        // mock resource
        Long connectionId = 1L;
        ConnectionDetailDTO connectionDetailDTO = fakeConnectionDetailDTO();
        Mockito.when(connectionService.getDetailById(ArgumentMatchers.eq(connectionId)))
                .thenReturn(connectionDetailDTO);
        Mockito.when(dataSystemResourceService.getById(ArgumentMatchers.eq(connectionDetailDTO.getSourceDataCollectionId())))
                .thenReturn(fakeSourceDataCollection());
        Mockito.when(dataSystemResourceService.getById(ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId())))
                .thenReturn(fakeSinkTable());
        Mockito.when(dataSystemResourceService.getDetailParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.TIDB_CLUSTER)))
                .thenReturn(fakeSinkClusterDetail());
        Mockito.when(dataSystemResourceService.getParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.TIDB_DATABASE)))
                .thenReturn(fakeSinkDatabase());
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.eq(connectionDetailDTO.getSinkInstanceId())))
                .thenReturn(fakeSinkInstance());
        
        Map<String, String> customConfig = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionId);
        Assertions.assertThat(customConfig.get("topics")).isEqualTo("source-topic-name");
        Assertions.assertThat(customConfig.get("destinations")).isEqualTo("sink-table");
        Assertions.assertThat(customConfig.get("connection.url")).isEqualTo("jdbc:mysql://sink-host:1111/sink-database");
        Assertions.assertThat(customConfig.get("connection.user")).isEqualTo("userName_1");
        Assertions.assertThat(customConfig.get("connection.password")).isEqualTo("password_1");
    }
    
    private DataSystemResourceDetailDTO fakeSinkInstance() {
        Map<String, DataSystemResourceConfigurationDTO> sinkInstanceConfig = new HashMap<>();
        DataSystemResourceConfigurationDTO hostConfig = new DataSystemResourceConfigurationDTO()
                .setName(CommonDataSystemResourceConfigurationDefinition.Endpoint.HOST_NAME)
                .setValue("sink-host");
        DataSystemResourceConfigurationDTO portConfig = new DataSystemResourceConfigurationDTO()
                .setName(CommonDataSystemResourceConfigurationDefinition.Endpoint.PORT_NAME)
                .setValue("1111");
        sinkInstanceConfig.put(hostConfig.getName(), hostConfig);
        sinkInstanceConfig.put(portConfig.getName(), portConfig);
        return new DataSystemResourceDetailDTO()
                .setDataSystemResourceConfigurations(sinkInstanceConfig);
    }
    
    private DataSystemResourceDTO fakeSinkDatabase() {
        return new DataSystemResourceDTO().setName("sink-database");
    }
    
    private DataSystemResourceDetailDTO fakeSinkClusterDetail() {
        Map<String, DataSystemResourceConfigurationDTO> dataSystemResourceConfigurations = new HashMap<>();
        DataSystemResourceConfigurationDTO userName = new DataSystemResourceConfigurationDTO()
                .setName(TidbDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName())
                .setValue("userName_1");
        DataSystemResourceConfigurationDTO password = new DataSystemResourceConfigurationDTO()
                .setName(TidbDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName())
                .setValue("password_1");
        dataSystemResourceConfigurations.put(userName.getName(), userName);
        dataSystemResourceConfigurations.put(password.getName(), password);
        
        return new DataSystemResourceDetailDTO()
                .setDataSystemResourceConfigurations(dataSystemResourceConfigurations);
    }
    
    private DataSystemResourceDTO fakeSinkTable() {
        return new DataSystemResourceDTO().setName("sink-table");
    }
    
    private DataSystemResourceDTO fakeSourceDataCollection() {
        return new DataSystemResourceDTO().setKafkaTopicName("source-topic-name");
    }
    
    private ConnectionDetailDTO fakeConnectionDetailDTO() {
        return new ConnectionDetailDTO()
                .setSourceDataCollectionId(11L)
                .setSinkDataCollectionId(21L)
                .setConnectionColumnConfigurations(new ArrayList<>());
    }
    
    private DataSystemResourceDTO fakeTable() {
        return new DataSystemResourceDTO()
                .setId(1L)
                .setName("table-1");
    }
    
    private DataSystemResourceDTO fakeDatabase() {
        return new DataSystemResourceDTO()
                .setId(1L)
                .setName("database-1");
    }
    
    private DataSystemResourceDTO fakeCluster() {
        return new DataSystemResourceDTO()
                .setId(1L)
                .setName("tidb-cluster-1");
    }
    
    @Test
    public void testGetConnectorSpecificConfigurationDefinitionsShouldReturnEmptyList() {
        Assertions.assertThat(dataSystemSinkConnectorService.getConnectorSpecificConfigurationDefinitions()).isEmpty();
    }
    
    @Test
    public void testGetSensitiveConfigurationNamesShouldAsExcept() {
        Assertions.assertThat(dataSystemSinkConnectorService.getSensitiveConfigurationNames())
                .containsExactlyElementsOf(TidbDataSystemConstant.Connector.Sink.Configuration.SENSITIVE_CONFIGURATION_NAMES);
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnTidb() {
        Assertions.assertThat(dataSystemSinkConnectorService.getDataSystemType()).isEqualTo(DataSystemType.TIDB);
    }
}
