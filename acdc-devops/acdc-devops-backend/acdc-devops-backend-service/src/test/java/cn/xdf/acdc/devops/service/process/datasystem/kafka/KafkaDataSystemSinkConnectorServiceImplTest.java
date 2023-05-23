package cn.xdf.acdc.devops.service.process.datasystem.kafka;

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
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemConstant.Connector.Sink.Configuration;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDataSystemSinkConnectorServiceImplTest {
    
    @Autowired
    @Qualifier("kafkaDataSystemSinkConnectorServiceImpl")
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
        Map<String, String> expectedDefaultConfiguration = new HashMap<>();
        expectedDefaultConfiguration.put("default-configuration-name-0", "default-configuration-value-0");
        expectedDefaultConfiguration.put("default-configuration-name-1", "default-configuration-value-1");
        
        Set<DefaultConnectorConfigurationDTO> defaultConnectorConfigurations = new HashSet<>();
        expectedDefaultConfiguration.forEach((key, value) -> {
            defaultConnectorConfigurations.add(
                    new DefaultConnectorConfigurationDTO().setName(key).setValue(value)
            );
        });
        
        ConnectorClassDetailDTO connectorClassDetail = new ConnectorClassDetailDTO().setDefaultConnectorConfigurations(defaultConnectorConfigurations);
        when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(eq(DataSystemType.KAFKA), eq(ConnectorType.SINK))).thenReturn(connectorClassDetail);
        
        Map<String, String> defaultConfiguration = dataSystemSinkConnectorService.getConnectorDefaultConfiguration();
        Assertions.assertThat(defaultConfiguration).isEqualTo(expectedDefaultConfiguration);
    }
    
    @Test
    public void testGenerateConnectorCustomConfigurationShouldAsExpect() {
        // just mock one column configuration here, more case should be tested in AbstractDataSystemSinkConnectorServiceTest
        ConnectionDetailDTO connectionDetail = generateConnectionDetail();
        when(connectionService.getDetailById(eq(connectionDetail.getId()))).thenReturn(connectionDetail);
        
        DataSystemResourceDTO sourceDataCollection = generateSourceDataCollection();
        when(dataSystemResourceService.getById(eq(connectionDetail.getSourceDataCollectionId()))).thenReturn(sourceDataCollection);
        
        DataSystemResourceDTO sinkTopic = generateSinkTopic();
        when(dataSystemResourceService.getById(eq(connectionDetail.getSinkDataCollectionId()))).thenReturn(sinkTopic);
        
        DataSystemResourceDetailDTO clusterDetail = generateClusterDetail();
        when(dataSystemResourceService.getDetailParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.KAFKA_CLUSTER))).thenReturn(clusterDetail);
        
        // execute
        final Map<String, String> customConfiguration = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionDetail.getId());
        
        // assert
        Map<String, String> desiredCustomConfiguration = new HashMap<>();
        desiredCustomConfiguration.put("topics", sourceDataCollection.getKafkaTopicName());
        desiredCustomConfiguration.put("destinations", sinkTopic.getName());
        
        desiredCustomConfiguration.put("destinations." + sinkTopic.getName() + ".delete.mode", "NONE");
        desiredCustomConfiguration.put("destinations." + sinkTopic.getName() + ".fields.whitelist", "source_column_name");
        desiredCustomConfiguration.put("destinations." + sinkTopic.getName() + ".fields.mapping", "source_column_name:sink_column_name");
        
        // smt
        desiredCustomConfiguration.put("transforms", "tombstoneFilter,unwrap");
        
        desiredCustomConfiguration.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
        desiredCustomConfiguration.put("transforms.tombstoneFilter.predicate", "isTombstone");
        // unwrap record from debezium json
        // we should only use this smt for records which has not been extracted
        desiredCustomConfiguration.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        desiredCustomConfiguration.put("transforms.unwrap.delete.handling.mode", "rewrite");
        desiredCustomConfiguration.put("transforms.unwrap.add.fields", "op,table");
        
        // predicate
        desiredCustomConfiguration.put("predicates", "isTombstone");
        desiredCustomConfiguration.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
        
        // converter
        desiredCustomConfiguration.put("sink.kafka.key.converter", "cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter");
        desiredCustomConfiguration.put("sink.kafka.value.converter", "cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter");
        
        KafkaConfigurationUtil.generateAdminClientConfiguration(clusterDetail).forEach((key, value) -> {
            desiredCustomConfiguration.put("sink.kafka." + key, value.toString());
        });
        
        Assertions.assertThat(customConfiguration).isEqualTo(desiredCustomConfiguration);
    }
    
    private ConnectionDetailDTO generateConnectionDetail() {
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        connectionColumnConfigurations.add(new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("source_column_name")
                .setSinkColumnName("sink_column_name")
        );
        
        return new ConnectionDetailDTO()
                .setId(1L)
                .setSourceConnectorId(2L)
                .setSinkDataCollectionId(3L)
                .setConnectionColumnConfigurations(connectionColumnConfigurations)
                .setSpecificConfiguration("{\"dataFormatType\":\"CDC_V1\"}");
    }
    
    private DataSystemResourceDTO generateSourceDataCollection() {
        return new DataSystemResourceDTO()
                .setId(2L)
                .setKafkaTopicName("topic_name");
    }
    
    private DataSystemResourceDTO generateSinkTopic() {
        return new DataSystemResourceDTO()
                .setId(3L)
                .setName("sink_topic");
    }
    
    private DataSystemResourceDetailDTO generateClusterDetail() {
        DataSystemResourceConfigurationDTO securityProtocol = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.SECURITY_PROTOCOL_CONFIG.getName())
                .setValue("SASL_PLAINTEXT");
        
        DataSystemResourceConfigurationDTO mechanism = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.SASL_MECHANISM.getName())
                .setValue("SCRAM-SHA-512");
        
        DataSystemResourceConfigurationDTO username = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.USERNAME.getName())
                .setValue("user_name");
        
        DataSystemResourceConfigurationDTO encryptedPassword = new DataSystemResourceConfigurationDTO()
                .setName(Cluster.PASSWORD.getName())
                .setValue(EncryptUtil.encrypt("password"));
        
        DataSystemResourceConfigurationDTO bootstrapServers = new DataSystemResourceConfigurationDTO()
                .setName(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                .setValue("6.6.6.2:6662");
        
        Map<String, DataSystemResourceConfigurationDTO> configurations = new HashMap<>();
        configurations.put(securityProtocol.getName(), securityProtocol);
        configurations.put(mechanism.getName(), mechanism);
        configurations.put(username.getName(), username);
        configurations.put(encryptedPassword.getName(), encryptedPassword);
        configurations.put(bootstrapServers.getName(), bootstrapServers);
        
        DataSystemResourceDetailDTO kafkaClusterDetail = new DataSystemResourceDetailDTO();
        kafkaClusterDetail.setId(4L);
        kafkaClusterDetail.setDataSystemResourceConfigurations(configurations);
        
        return kafkaClusterDetail;
    }
    
    @Test
    public void testGetConnectorSpecificConfigurationDefinitionsShouldAsExpect() {
        Assertions.assertThat(dataSystemSinkConnectorService.getConnectorSpecificConfigurationDefinitions())
                .isEqualTo(KafkaSinkConnectorSpecificConfigurationDefinition.Sink.SPECIFIC_CONFIGURATION_DEFINITIONS);
    }
    
    @Test
    public void testGetSensitiveConfigurationNamesShouldAsExcept() {
        Assertions.assertThat(dataSystemSinkConnectorService.getSensitiveConfigurationNames())
                .isEqualTo(Configuration.SENSITIVE_CONFIGURATION_NAMES);
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnKafka() {
        Assertions.assertThat(dataSystemSinkConnectorService.getDataSystemType()).isEqualTo(DataSystemType.KAFKA);
    }
    
    @Test
    public void testGetConnectorClassShouldAsExpect() {
        dataSystemSinkConnectorService.getConnectorClass();
        
        ArgumentCaptor<DataSystemType> dataSystemTypeCaptor = ArgumentCaptor.forClass(DataSystemType.class);
        ArgumentCaptor<ConnectorType> connectorTypeCaptor = ArgumentCaptor.forClass(ConnectorType.class);
        
        Mockito.verify(connectorClassService).getDetailByDataSystemTypeAndConnectorType(dataSystemTypeCaptor.capture(), connectorTypeCaptor.capture());
        Assertions.assertThat(dataSystemTypeCaptor.getValue()).isEqualTo(DataSystemType.KAFKA);
        Assertions.assertThat(connectorTypeCaptor.getValue()).isEqualTo(ConnectorType.SINK);
    }
    
    @Test
    public void testGenerateSpecificConfigurationWhenCdcV1ShouldAsExpect() {
        final Map<String, String> configuration = ((KafkaDataSystemSinkConnectorServiceImpl) dataSystemSinkConnectorService).generateSpecificConfiguration("{\"dataFormatType\":\"CDC_V1\"}");
        
        Map<String, String> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put("transforms", "tombstoneFilter,unwrap");
        
        expectedConfiguration.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
        expectedConfiguration.put("transforms.tombstoneFilter.predicate", "isTombstone");
        // unwrap record from debezium json
        // we should only use this smt for records which has not been extracted
        expectedConfiguration.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        expectedConfiguration.put("transforms.unwrap.delete.handling.mode", "rewrite");
        expectedConfiguration.put("transforms.unwrap.add.fields", "op,table");
        
        // predicate
        expectedConfiguration.put("predicates", "isTombstone");
        expectedConfiguration.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
        
        // converter
        expectedConfiguration.put("sink.kafka.key.converter", "cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter");
        expectedConfiguration.put("sink.kafka.value.converter", "cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter");
        
        Assertions.assertThat(configuration).isEqualTo(expectedConfiguration);
    }
    
    @Test
    public void testGenerateSpecificConfigurationWhenJsonShouldAsExpect() {
        final Map<String, String> configuration = ((KafkaDataSystemSinkConnectorServiceImpl) dataSystemSinkConnectorService).generateSpecificConfiguration("{\"dataFormatType\":\"JSON\"}");
        
        Map<String, String> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put("transforms", "tombstoneFilter,dateToString");
        
        expectedConfiguration.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
        expectedConfiguration.put("transforms.tombstoneFilter.predicate", "isTombstone");
        
        // predicate
        expectedConfiguration.put("predicates", "isTombstone");
        expectedConfiguration.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
        
        // date to string smt
        expectedConfiguration.put("transforms.dateToString.type", "cn.xdf.acdc.connect.transforms.format.date.DateToString");
        expectedConfiguration.put("transforms.dateToString.zoned.timestamp.formatter", "zoned");
        
        // converter
        expectedConfiguration.put("sink.kafka.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        expectedConfiguration.put("sink.kafka.key.converter.schemas.enable", "false");
        expectedConfiguration.put("sink.kafka.key.converter.decimal.format", "NUMERIC");
        expectedConfiguration.put("sink.kafka.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        expectedConfiguration.put("sink.kafka.value.converter.schemas.enable", "false");
        expectedConfiguration.put("sink.kafka.value.converter.decimal.format", "NUMERIC");
        Assertions.assertThat(configuration).isEqualTo(expectedConfiguration);
    }
    
    @Test
    public void testGenerateSpecificConfigurationWhenSchemaLessJsonShouldAsExpect() {
        final Map<String, String> configuration = ((KafkaDataSystemSinkConnectorServiceImpl) dataSystemSinkConnectorService).generateSpecificConfiguration("{\"dataFormatType\":\"SCHEMA_LESS_JSON\"}");
        
        Map<String, String> expectedConfiguration = new HashMap<>();
        expectedConfiguration.put("transforms", "tombstoneFilter,dateToString,unwrap");
        
        expectedConfiguration.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
        expectedConfiguration.put("transforms.tombstoneFilter.predicate", "isTombstone");
        
        // date to string smt
        expectedConfiguration.put("transforms.dateToString.type", "cn.xdf.acdc.connect.transforms.format.date.DateToString");
        expectedConfiguration.put("transforms.dateToString.zoned.timestamp.formatter", "local");
        
        // unwrap record from debezium json
        // we should only use this smt for records which has not been extracted
        expectedConfiguration.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        expectedConfiguration.put("transforms.unwrap.delete.handling.mode", "rewrite");
        expectedConfiguration.put("transforms.unwrap.add.fields", "op,table");
        
        // predicate
        expectedConfiguration.put("predicates", "isTombstone");
        expectedConfiguration.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
        
        // converter
        expectedConfiguration.put("sink.kafka.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        expectedConfiguration.put("sink.kafka.key.converter.schemas.enable", "false");
        expectedConfiguration.put("sink.kafka.key.converter.decimal.format", "NUMERIC");
        expectedConfiguration.put("sink.kafka.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        expectedConfiguration.put("sink.kafka.value.converter.schemas.enable", "false");
        expectedConfiguration.put("sink.kafka.value.converter.decimal.format", "NUMERIC");
        
        Assertions.assertThat(configuration).isEqualTo(expectedConfiguration);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGenerateSpecificConfigurationShouldErrorWhenFormatIsWrong() {
        ((KafkaDataSystemSinkConnectorServiceImpl) dataSystemSinkConnectorService).generateSpecificConfiguration("{\"dataFormatType\":\"NOT_EXISTS_TYPE\"}");
    }
}
