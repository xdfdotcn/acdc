package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DefaultConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.config.TopicProperties;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.Mysql;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MysqlDataSystemSourceConnectorServiceImplTest {
    
    @Autowired
    @Qualifier("mysqlDataSystemSourceConnectorServiceImpl")
    private DataSystemSourceConnectorService dataSystemSourceConnectorService;
    
    @Autowired
    private TopicProperties topicProperties;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private KafkaClusterService kafkaClusterService;
    
    @MockBean
    private KafkaHelperService kafkaHelperService;
    
    @MockBean
    private ConnectorClassService connectorClassService;
    
    @MockBean(name = "mysqlDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Before
    public void setUp() throws Exception {
    
    }
    
    @Test
    public void testVerifyDataSystemMetadataShouldPass() {
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDTO().setId(1L)
        );
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE), eq(Instance.ROLE_TYPE.getName()), eq(MysqlInstanceRoleType.DATA_SOURCE.name())))
                .thenReturn(Arrays.asList(new DataSystemResourceDetailDTO()));
        
        dataSystemSourceConnectorService.verifyDataSystemMetadata(1L);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testVerifyDataSystemMetadataShouldErrorWhenThereIsNoDataSourceInstance() {
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDTO().setId(1L)
        );
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE), anyString(), anyString()))
                .thenReturn(Lists.emptyList());
        
        dataSystemSourceConnectorService.verifyDataSystemMetadata(1L);
    }
    
    @Test
    public void testAfterConnectorCreationShouldCreateSchemaChangeAndHistoryTopic() {
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDTO().setId(1L).setName("cluster")
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(
                new DataSystemResourceDTO().setId(2L).setName("database")
        );
        when(kafkaClusterService.getACDCKafkaCluster()).thenReturn(new KafkaClusterDTO().setId(1L));
        
        dataSystemSourceConnectorService.afterConnectorCreation(1L);
        
        ArgumentCaptor<String> topicNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> numPartitionsCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Short> replicationFactorCaptor = ArgumentCaptor.forClass(Short.class);
        ArgumentCaptor<Map<String, String>> topicConfigCaptor = ArgumentCaptor.forClass(Map.class);
        
        Mockito.verify(kafkaHelperService, times(2)).createTopic(topicNameCaptor.capture(), numPartitionsCaptor.capture(), replicationFactorCaptor.capture(), topicConfigCaptor.capture(), anyMap());
        
        // schema history topic
        Assertions.assertThat(topicNameCaptor.getAllValues().get(0)).isEqualTo("schema_history-source-mysql-1-database");
        Assertions.assertThat(numPartitionsCaptor.getAllValues().get(0)).isEqualTo(Integer.valueOf(topicProperties.getSchemaHistory().getPartitions()));
        Assertions.assertThat(replicationFactorCaptor.getAllValues().get(0)).isEqualTo(Short.valueOf(topicProperties.getSchemaHistory().getReplicationFactor()));
        Assertions.assertThat(topicConfigCaptor.getAllValues().get(0)).isEqualTo(topicProperties.getSchemaHistory().getConfigs());
        
        // schema change topic
        Assertions.assertThat(topicNameCaptor.getAllValues().get(1)).isEqualTo("source-mysql-1-database");
        Assertions.assertThat(numPartitionsCaptor.getAllValues().get(1)).isEqualTo(Integer.valueOf(Integer.valueOf(topicProperties.getSchemaChange().getPartitions())));
        Assertions.assertThat(replicationFactorCaptor.getAllValues().get(1)).isEqualTo(Short.valueOf(topicProperties.getSchemaChange().getReplicationFactor()));
        Assertions.assertThat(topicConfigCaptor.getAllValues().get(1)).isEqualTo(topicProperties.getSchemaChange().getConfigs());
    }
    
    @Test
    public void testGenerateConnectorNameShouldAsExpect() {
        // check generated connector name as expect
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDTO().setId(1L).setName("cluster")
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(
                new DataSystemResourceDTO().setId(2L).setName("database")
        );
        String connectorName = dataSystemSourceConnectorService.generateConnectorName(1L);
        Assertions.assertThat(connectorName).isEqualTo("source-mysql-1-database");
    }
    
    @Test
    public void testGenerateKafkaTopicNameShouldAsExpect() {
        // check generated topic name as expect
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDTO().setId(1L).setName("cluster")
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(
                new DataSystemResourceDTO().setId(2L).setName("database")
        );
        when(dataSystemResourceService.getById(anyLong())).thenReturn(
                new DataSystemResourceDTO().setId(3L).setName("table")
        );
        String kafkaTopicName = dataSystemSourceConnectorService.generateKafkaTopicName(3L);
        Assertions.assertThat(kafkaTopicName).isEqualTo("source-mysql-1-database-table");
    }
    
    @Test
    public void testGenerateConnectorCustomConfigurationShouldAsExpect() {
        // do mock
        List<Long> tableIds = Arrays.asList(1L, 2L, 3L);
        for (Long each : tableIds) {
            when(dataSystemResourceService.getById(eq(each))).thenReturn(new DataSystemResourceDTO().setName("table_" + each));
            
            // table definition
            List<DataFieldDefinition> dataFieldDefinitions = new ArrayList();
            // table 1 has a primary key, others have unique key
            if (each == 1L) {
                dataFieldDefinitions.add(new DataFieldDefinition("table_" + each + "-field_1", "bigint", Sets.newHashSet(Mysql.PK_INDEX_NAME)));
            } else {
                dataFieldDefinitions.add(new DataFieldDefinition("table_" + each + "-field_1", "bigint", Sets.newHashSet("unique_key_1", "unique_key_2")));
                dataFieldDefinitions.add(new DataFieldDefinition("table_" + each + "-field_2", "bigint", Sets.newHashSet("unique_key_1", "unique_key_2")));
            }
            when(dataSystemMetadataService.getDataCollectionDefinition(eq(each))).thenReturn(new DataCollectionDefinition("table_" + each, dataFieldDefinitions));
        }
        DataSystemResourceDetailDTO cluster = generateCluster();
        when(dataSystemResourceService.getDetailParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(cluster);
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDTO().setId(cluster.getId())
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(generateDatabase());
        when(dataSystemResourceService
                .getDetailChildren(eq(cluster.getId()), eq(DataSystemResourceType.MYSQL_INSTANCE), eq(Instance.ROLE_TYPE.getName()), eq(MysqlInstanceRoleType.DATA_SOURCE.name())))
                .thenReturn(Arrays.asList(generateDataSourceInstance()));
        when(kafkaClusterService.getACDCKafkaCluster()).thenReturn(
                new KafkaClusterDTO()
                        .setBootstrapServers("6.6.6.2:6662")
                        .setSecurityConfiguration(
                                "{\"security.protocol\":\"SASL_PLAINTEXT\","
                                        + "\"sasl.mechanism\":\"SCRAM-SHA-512\","
                                        + "\"sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"user\\\" password=\\\"password\\\"\"}")
        );
        
        // execute
        final Map<String, String> customConfiguration = dataSystemSourceConnectorService.generateConnectorCustomConfiguration(tableIds);
        
        // assert
        Map<String, String> desiredCustomConfiguration = new HashMap<>();
        desiredCustomConfiguration.put("database.server.name", "source-mysql-1-database");
        desiredCustomConfiguration.put("database.history.consumer.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"password\"");
        desiredCustomConfiguration.put("database.history.consumer.security.protocol", "SASL_PLAINTEXT");
        desiredCustomConfiguration.put("database.history.consumer.sasl.mechanism", "SCRAM-SHA-512");
        desiredCustomConfiguration.put("database.history.producer.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"password\"");
        desiredCustomConfiguration.put("database.history.producer.security.protocol", "SASL_PLAINTEXT");
        desiredCustomConfiguration.put("database.history.producer.sasl.mechanism", "SCRAM-SHA-512");
        desiredCustomConfiguration.put("database.history.kafka.bootstrap.servers", "6.6.6.2:6662");
        desiredCustomConfiguration.put("database.history.kafka.topic", "schema_history-source-mysql-1-database");
        desiredCustomConfiguration.put("database.hostname", "6.6.6.2");
        desiredCustomConfiguration.put("database.port", "6662");
        desiredCustomConfiguration.put("database.user", "user");
        desiredCustomConfiguration.put("database.password", "password");
        desiredCustomConfiguration.put("database.include", "database");
        desiredCustomConfiguration.put("table.include.list", "database.table_1,database.table_2,database.table_3");
        desiredCustomConfiguration.put("message.key.columns", "database.table_1:table_1-field_1;database.table_2:table_2-field_1,table_2-field_2;database.table_3:table_3-field_1,table_3-field_2;");
        
        Assertions.assertThat(customConfiguration).isEqualTo(desiredCustomConfiguration);
    }
    
    private DataSystemResourceDetailDTO generateCluster() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap();
        clusterConfigurations.put(Cluster.USERNAME.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Cluster.USERNAME.getName())
                .setValue("user"));
        clusterConfigurations.put(Cluster.PASSWORD.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Cluster.PASSWORD.getName())
                .setValue("password"));
        
        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("cluster")
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemResourceConfigurations(clusterConfigurations);
    }
    
    private DataSystemResourceDTO generateDatabase() {
        return new DataSystemResourceDTO()
                .setId(2L)
                .setName("database")
                .setResourceType(DataSystemResourceType.MYSQL_DATABASE);
    }
    
    private DataSystemResourceDetailDTO generateDataSourceInstance() {
        Map<String, DataSystemResourceConfigurationDTO> dataSourceInstanceConfigurations = new HashMap();
        dataSourceInstanceConfigurations.put(Instance.HOST.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Instance.HOST.getName())
                .setValue("6.6.6.2"));
        dataSourceInstanceConfigurations.put(Instance.PORT.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Instance.PORT.getName())
                .setValue("6662"));
        dataSourceInstanceConfigurations.put(Instance.ROLE_TYPE.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Instance.ROLE_TYPE.getName())
                .setValue(MysqlInstanceRoleType.DATA_SOURCE.name()));
        
        return new DataSystemResourceDetailDTO()
                .setId(3L)
                .setName("data_source")
                .setResourceType(DataSystemResourceType.MYSQL_INSTANCE)
                .setDataSystemResourceConfigurations(dataSourceInstanceConfigurations);
    }
    
    @Test
    public void testGetConnectorDefaultConfigurationShouldAsExpect() {
        Map<String, String> expectedDefaultConfiguration = new HashMap<>();
        expectedDefaultConfiguration.put("default-configuration-name-0", "default-configuration-value-0");
        expectedDefaultConfiguration.put("default-configuration-name-1", "default-configuration-value-1");
        
        Set<DefaultConnectorConfigurationDTO> defaultConnectorConfigurations = new HashSet<>();
        expectedDefaultConfiguration.forEach((key, value) -> {
            defaultConnectorConfigurations.add(
                    new DefaultConnectorConfigurationDTO()
                            .setName(key)
                            .setValue(value)
            );
        });
        
        ConnectorClassDetailDTO connectorClassDetail = new ConnectorClassDetailDTO()
                .setDefaultConnectorConfigurations(defaultConnectorConfigurations);
        when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(eq(DataSystemType.MYSQL), eq(ConnectorType.SOURCE))).thenReturn(connectorClassDetail);
        
        Map<String, String> defaultConfiguration = dataSystemSourceConnectorService.getConnectorDefaultConfiguration();
        Assertions.assertThat(defaultConfiguration).isEqualTo(expectedDefaultConfiguration);
    }
    
    @Test
    public void testGetImmutableConfigurationNamesShouldAsExpect() {
        Assertions.assertThat(dataSystemSourceConnectorService.getImmutableConfigurationNames())
                .isEqualTo(Sets.newHashSet("database.server.name", "database.history.kafka.topic"));
    }
    
    @Test
    public void testGetSensitiveConfigurationNamesShouldAsExpect() {
        Assertions.assertThat(dataSystemSourceConnectorService.getSensitiveConfigurationNames())
                .isEqualTo(Sets.newHashSet("database.password", "database.history.producer.sasl.jaas.config", "database.history.consumer.sasl.jaas.config"));
    }
    
    @Test
    public void testGetConnectorDataSystemResourceTypeShouldReturnDatabase() {
        Assertions.assertThat(dataSystemSourceConnectorService.getConnectorDataSystemResourceType()).isEqualTo(DataSystemResourceType.MYSQL_DATABASE);
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnMysql() {
        Assertions.assertThat(dataSystemSourceConnectorService.getDataSystemType()).isEqualTo(DataSystemType.MYSQL);
    }
    
    @Test
    public void testGetConnectorClassShouldAsExpect() {
        dataSystemSourceConnectorService.getConnectorClass();
        
        ArgumentCaptor<DataSystemType> dataSystemTypeCaptor = ArgumentCaptor.forClass(DataSystemType.class);
        ArgumentCaptor<ConnectorType> connectorTypeCaptor = ArgumentCaptor.forClass(ConnectorType.class);
        
        Mockito.verify(connectorClassService).getDetailByDataSystemTypeAndConnectorType(dataSystemTypeCaptor.capture(), connectorTypeCaptor.capture());
        Assertions.assertThat(dataSystemTypeCaptor.getValue()).isEqualTo(DataSystemType.MYSQL);
        Assertions.assertThat(connectorTypeCaptor.getValue()).isEqualTo(ConnectorType.SOURCE);
    }
}
