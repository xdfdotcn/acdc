package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
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
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class StarRocksDataSystemSinkConnectorServiceImplTest {
    
    @Autowired
    @Qualifier("starRocksDataSystemSinkConnectorServiceImpl")
    private DataSystemSinkConnectorService dataSystemSinkConnectorService;
    
    @MockBean
    private ConnectorClassService connectorClassService;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private ConnectionService connectionService;
    
    @MockBean
    @Qualifier("starRocksDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Before
    public void setUp() throws Exception {
    
    }
    
    @Test
    public void testGetConnectorDefaultConfigurationShouldAsExpect() {
        Mockito.when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(
                        ArgumentMatchers.eq(DataSystemType.STARROCKS), ArgumentMatchers.eq(ConnectorType.SINK)))
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
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_CLUSTER)))
                .thenReturn(fakeSinkClusterDetail());
        Mockito.when(dataSystemResourceService.getParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_DATABASE)))
                .thenReturn(fakeSinkDatabase());
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.eq(connectionDetailDTO.getSinkInstanceId())))
                .thenReturn(fakeSinkInstance());
        Mockito.when(dataSystemMetadataService.getDataCollectionDefinition(ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId())))
                .thenReturn(fakePrimaryKeyDataCollectionDefinition());
        Mockito.when(dataSystemResourceService.getDetailChildren(ArgumentMatchers.eq(111L), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_FRONTEND)))
                .thenReturn(fakeFrontEnds());
        
        Map<String, String> customConfig = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionId);
        Assertions.assertThat(customConfig.get("topics")).isEqualTo("source-topic-name");
        Assertions.assertThat(customConfig.get("database.name")).isEqualTo("sink-database");
        Assertions.assertThat(customConfig.get("table.name")).isEqualTo("sink-table");
        Assertions.assertThat(customConfig.get("load.url")).isEqualTo("sink-host:2222");
        Assertions.assertThat(customConfig.get("username")).isEqualTo("userName_1");
        Assertions.assertThat(customConfig.get("password")).isEqualTo("password_1");
        Assertions.assertThat(customConfig.get("transforms")).isEqualTo("dateToString,unwrap,valueMapperSource,replaceField");
        Assertions.assertThat(customConfig.get("transforms.dateToString.type")).isEqualTo("cn.xdf.acdc.connect.transforms.format.date.DateToString");
        Assertions.assertThat(customConfig.get("transforms.dateToString.zoned.timestamp.formatter")).isEqualTo("local");
        Assertions.assertThat(customConfig.get("transforms.unwrap.type")).isEqualTo("io.debezium.transforms.ExtractNewRecordState");
        Assertions.assertThat(customConfig.get("transforms.unwrap.delete.handling.mode")).isEqualTo("rewrite");
        Assertions.assertThat(customConfig.get("transforms.unwrap.add.fields")).isEqualTo("op");
        Assertions.assertThat(customConfig.get("transforms.valueMapperSource.type")).isEqualTo("cn.xdf.acdc.connect.transforms.valuemapper.StringValueMapper");
        Assertions.assertThat(customConfig.get("transforms.valueMapperSource.mappings")).isEqualTo("c:0,u:0,d:1");
        Assertions.assertThat(customConfig.get("transforms.valueMapperSource.field")).isEqualTo("__op");
        Assertions.assertThat(customConfig.get("transforms.replaceField.type")).isEqualTo("org.apache.kafka.connect.transforms.ReplaceField$Value");
        Assertions.assertThat(customConfig.get("transforms.replaceField.whitelist")).isEqualTo("code,__op");
        Assertions.assertThat(customConfig.get("transforms.replaceField.renames")).isEqualTo("code:rename_code");
        Assertions.assertThat(customConfig.get("sink.columns")).isEqualTo("rename_code,__op");
    }
    
    @Test
    public void testGenerateConnectorCustomConfigurationShouldExcludeOpWithoutPrimaryKey() {
        Long connectionId = 1L;
        ConnectionDetailDTO connectionDetailDTO = fakeConnectionDetailDTOWithoutPrimaryKey();
        Mockito.when(connectionService.getDetailById(ArgumentMatchers.eq(connectionId)))
                .thenReturn(connectionDetailDTO);
        Mockito.when(dataSystemResourceService.getById(ArgumentMatchers.eq(connectionDetailDTO.getSourceDataCollectionId())))
                .thenReturn(fakeSourceDataCollection());
        Mockito.when(dataSystemResourceService.getById(ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId())))
                .thenReturn(fakeSinkTable());
        Mockito.when(dataSystemResourceService.getDetailParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_CLUSTER)))
                .thenReturn(fakeSinkClusterDetail());
        Mockito.when(dataSystemResourceService.getParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_DATABASE)))
                .thenReturn(fakeSinkDatabase());
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.eq(connectionDetailDTO.getSinkInstanceId())))
                .thenReturn(fakeSinkInstance());
        Mockito.when(dataSystemMetadataService.getDataCollectionDefinition(ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId())))
                .thenReturn(fakeUniqueKeyDataCollectionDefinition());
        
        Map<String, String> customConfig = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionId);
        Assertions.assertThat(customConfig.get("transforms.replaceField.whitelist")).isEqualTo("code");
        Assertions.assertThat(customConfig.get("transforms.replaceField.renames")).isEqualTo("code:rename_code");
        Assertions.assertThat(customConfig.get("sink.columns")).isEqualTo("rename_code");
    }
    
    private List<DataSystemResourceDetailDTO> fakeFrontEnds() {
        Map<String, DataSystemResourceConfigurationDTO> sinkInstanceConfig = new HashMap<>();
        DataSystemResourceConfigurationDTO hostConfig = new DataSystemResourceConfigurationDTO()
                .setName("host")
                .setValue("sink-host");
        DataSystemResourceConfigurationDTO jdbcPortConfig = new DataSystemResourceConfigurationDTO()
                .setName("jdbcPort")
                .setValue("1111");
        DataSystemResourceConfigurationDTO httpPortConfig = new DataSystemResourceConfigurationDTO()
                .setName("httpPort")
                .setValue("2222");
        sinkInstanceConfig.put(hostConfig.getName(), hostConfig);
        sinkInstanceConfig.put(jdbcPortConfig.getName(), jdbcPortConfig);
        sinkInstanceConfig.put(httpPortConfig.getName(), httpPortConfig);
        final DataSystemResourceDetailDTO dataSystemResourceDetailDTO = new DataSystemResourceDetailDTO().setDataSystemResourceConfigurations(sinkInstanceConfig);
        return Lists.newArrayList(dataSystemResourceDetailDTO);
    }
    
    private DataCollectionDefinition fakeUniqueKeyDataCollectionDefinition() {
        final Properties properties = new Properties();
        properties.put("TABLE_MODEL", "UNIQUE_KEYS");
        return new DataCollectionDefinition("table", new ArrayList<>(), properties);
    }
    
    private DataCollectionDefinition fakePrimaryKeyDataCollectionDefinition() {
        final Properties properties = new Properties();
        properties.put("TABLE_MODEL", "PRIMARY_KEYS");
        return new DataCollectionDefinition("table", new ArrayList<>(), properties);
    }
    
    private DataSystemResourceDetailDTO fakeSinkInstance() {
        Map<String, DataSystemResourceConfigurationDTO> sinkInstanceConfig = new HashMap<>();
        DataSystemResourceConfigurationDTO hostConfig = new DataSystemResourceConfigurationDTO()
                .setName("host")
                .setValue("sink-host");
        DataSystemResourceConfigurationDTO jdbcPortConfig = new DataSystemResourceConfigurationDTO()
                .setName("jdbcPort")
                .setValue("1111");
        DataSystemResourceConfigurationDTO httpPortConfig = new DataSystemResourceConfigurationDTO()
                .setName("httpPort")
                .setValue("2222");
        sinkInstanceConfig.put(hostConfig.getName(), hostConfig);
        sinkInstanceConfig.put(jdbcPortConfig.getName(), jdbcPortConfig);
        sinkInstanceConfig.put(httpPortConfig.getName(), httpPortConfig);
        return new DataSystemResourceDetailDTO()
                .setDataSystemResourceConfigurations(sinkInstanceConfig);
    }
    
    private DataSystemResourceDTO fakeSinkDatabase() {
        return new DataSystemResourceDTO().setName("sink-database");
    }
    
    private DataSystemResourceDetailDTO fakeSinkClusterDetail() {
        Map<String, DataSystemResourceConfigurationDTO> dataSystemResourceConfigurations = new HashMap<>();
        DataSystemResourceConfigurationDTO userName = new DataSystemResourceConfigurationDTO()
                .setName("username")
                .setValue("userName_1");
        DataSystemResourceConfigurationDTO password = new DataSystemResourceConfigurationDTO()
                .setName("password")
                .setValue("password_1");
        dataSystemResourceConfigurations.put(userName.getName(), userName);
        dataSystemResourceConfigurations.put(password.getName(), password);
        
        return new DataSystemResourceDetailDTO()
                .setId(111L)
                .setDataSystemResourceConfigurations(dataSystemResourceConfigurations);
    }
    
    private DataSystemResourceDTO fakeSinkTable() {
        return new DataSystemResourceDTO().setName("sink-table");
    }
    
    private DataSystemResourceDTO fakeSourceDataCollection() {
        return new DataSystemResourceDTO().setKafkaTopicName("source-topic-name");
    }
    
    private ConnectionDetailDTO fakeConnectionDetailDTOWithoutPrimaryKey() {
        final ConnectionColumnConfigurationDTO uniKeyColumn = new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("code")
                .setSinkColumnName("rename_code")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("Unique"));
        return new ConnectionDetailDTO()
                .setSourceDataCollectionId(11L)
                .setSinkDataCollectionId(21L)
                .setConnectionColumnConfigurations(Lists.newArrayList(uniKeyColumn));
    }
    
    private ConnectionDetailDTO fakeConnectionDetailDTO() {
        final ConnectionColumnConfigurationDTO uniKeyColumn = new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("code")
                .setSinkColumnName("rename_code")
                .setSinkColumnUniqueIndexNames(Sets.newHashSet("PRIMARY"));
        return new ConnectionDetailDTO()
                .setSourceDataCollectionId(11L)
                .setSinkDataCollectionId(21L)
                .setConnectionColumnConfigurations(Lists.newArrayList(uniKeyColumn));
    }
    
    @Test
    public void testGetConnectorSpecificConfigurationDefinitionsShouldReturnEmptyList() {
        Assertions.assertThat(dataSystemSinkConnectorService.getConnectorSpecificConfigurationDefinitions()).isEmpty();
    }
    
    @Test
    public void testGetSensitiveConfigurationNamesShouldAsExcept() {
        Assertions.assertThat(dataSystemSinkConnectorService.getSensitiveConfigurationNames())
                .containsExactlyElementsOf(StarRocksDataSystemConstant.Connector.Sink.Configuration.SENSITIVE_CONFIGURATION_NAMES);
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnStarRocks() {
        Assertions.assertThat(dataSystemSinkConnectorService.getDataSystemType()).isEqualTo(DataSystemType.STARROCKS);
    }
    
    @Test
    public void testGenerateConnectorCustomConfigurationWithInnerSpecificationShouldAsExpected() {
        // mock resource
        Long connectionId = 1L;
        ConnectionDetailDTO connectionDetailDTO = fakeConnectionDetailDTO()
                .setSpecificConfiguration("{\"INNER_CONNECTION_TYPE\":\"STARROCKS_SOURCE\"}");
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = connectionDetailDTO.getConnectionColumnConfigurations();
        connectionColumnConfigurations.add(
                new ConnectionColumnConfigurationDTO()
                        .setSourceColumnName("__deleted")
                        .setSinkColumnName("__is_deleted")
        );
        connectionColumnConfigurations.add(
                new ConnectionColumnConfigurationDTO()
                        .setSourceColumnName("__kafka_record_offset")
                        .setSinkColumnName("__event_offset")
        );
        Mockito.when(connectionService.getDetailById(ArgumentMatchers.eq(connectionId)))
                .thenReturn(connectionDetailDTO);
        Mockito.when(dataSystemResourceService.getById(ArgumentMatchers.eq(connectionDetailDTO.getSourceDataCollectionId())))
                .thenReturn(fakeSourceDataCollection());
        Mockito.when(dataSystemResourceService.getById(ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId())))
                .thenReturn(fakeSinkTable());
        Mockito.when(dataSystemResourceService.getDetailParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_CLUSTER)))
                .thenReturn(fakeSinkClusterDetail());
        Mockito.when(dataSystemResourceService.getParent(
                        ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId()), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_DATABASE)))
                .thenReturn(fakeSinkDatabase());
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.eq(connectionDetailDTO.getSinkInstanceId())))
                .thenReturn(fakeSinkInstance());
        Mockito.when(dataSystemMetadataService.getDataCollectionDefinition(ArgumentMatchers.eq(connectionDetailDTO.getSinkDataCollectionId())))
                .thenReturn(fakePrimaryKeyDataCollectionDefinition());
        Mockito.when(dataSystemResourceService.getDetailChildren(ArgumentMatchers.eq(111L), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_FRONTEND)))
                .thenReturn(fakeFrontEnds());
        
        Map<String, String> customConfig = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionId);
        Assertions.assertThat(customConfig.get("topics")).isEqualTo("source-topic-name");
        Assertions.assertThat(customConfig.get("database.name")).isEqualTo("sink-database");
        Assertions.assertThat(customConfig.get("table.name")).isEqualTo("sink-table");
        Assertions.assertThat(customConfig.get("load.url")).isEqualTo("sink-host:2222");
        Assertions.assertThat(customConfig.get("username")).isEqualTo("userName_1");
        Assertions.assertThat(customConfig.get("password")).isEqualTo("password_1");
        Assertions.assertThat(customConfig.get("transforms")).isEqualTo("dateToString,unwrap,valueMapperSource,replaceField,insertField");
        Assertions.assertThat(customConfig.get("transforms.dateToString.type")).isEqualTo("cn.xdf.acdc.connect.transforms.format.date.DateToString");
        Assertions.assertThat(customConfig.get("transforms.dateToString.zoned.timestamp.formatter")).isEqualTo("local");
        Assertions.assertThat(customConfig.get("transforms.unwrap.type")).isEqualTo("io.debezium.transforms.ExtractNewRecordState");
        Assertions.assertThat(customConfig.get("transforms.unwrap.delete.handling.mode")).isEqualTo("rewrite");
        Assertions.assertThat(customConfig.get("transforms.unwrap.add.fields")).isEqualTo("op");
        Assertions.assertThat(customConfig.get("transforms.valueMapperSource.type")).isEqualTo("cn.xdf.acdc.connect.transforms.valuemapper.StringValueMapper");
        Assertions.assertThat(customConfig.get("transforms.valueMapperSource.mappings")).isEqualTo("c:0,u:0,d:0");
        Assertions.assertThat(customConfig.get("transforms.valueMapperSource.field")).isEqualTo("__op");
        Assertions.assertThat(customConfig.get("transforms.replaceField.type")).isEqualTo("org.apache.kafka.connect.transforms.ReplaceField$Value");
        Assertions.assertThat(customConfig.get("transforms.replaceField.whitelist")).isEqualTo("code,__deleted,__op");
        Assertions.assertThat(customConfig.get("transforms.replaceField.renames")).isEqualTo("code:rename_code,__deleted:__is_deleted,__kafka_record_offset:__event_offset");
        Assertions.assertThat(customConfig.get("transforms.insertField.type")).isEqualTo("org.apache.kafka.connect.transforms.InsertField$Value");
        Assertions.assertThat(customConfig.get("transforms.insertField.offset.field")).isEqualTo("__event_offset");
        Assertions.assertThat(customConfig.get("sink.columns")).isEqualTo("rename_code,__is_deleted,__event_offset,__op");
    }
}
