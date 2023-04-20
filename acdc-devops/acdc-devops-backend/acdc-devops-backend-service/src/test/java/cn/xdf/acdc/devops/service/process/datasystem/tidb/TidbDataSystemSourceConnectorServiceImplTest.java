package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.dto.ConnectorClassDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.DefaultConnectorConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectorType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSourceConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlInstanceRoleType;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
import com.google.common.collect.Sets;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class TidbDataSystemSourceConnectorServiceImplTest {

    @Autowired
    @Qualifier("tidbDataSystemSourceConnectorServiceImpl")
    private DataSystemSourceConnectorService tidbDataSystemSourceConnectorServiceImpl;

    @MockBean
    private DataSystemResourceService dataSystemResourceService;

    @MockBean
    private KafkaClusterService kafkaClusterService;

    @MockBean
    private KafkaTopicService kafkaTopicService;

    @MockBean
    @Qualifier("tidbDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;

    @MockBean
    private ConnectorClassService connectorClassService;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testBeforeConnectorCreationShouldSaveTicdcTopicAndRelationsWithKafkaClusterAndDatabase() {
        Long tableId = 111L;
        Mockito.when(dataSystemResourceService.getParent(ArgumentMatchers.eq(tableId), ArgumentMatchers.eq(DataSystemResourceType.TIDB_DATABASE)))
                .thenReturn(fakeDatabase());
        Mockito.when(dataSystemResourceService.getParent(ArgumentMatchers.eq(tableId), ArgumentMatchers.eq(DataSystemResourceType.TIDB_CLUSTER)))
                .thenReturn(fakeCluster());
        Mockito.when(kafkaClusterService.getTICDCKafkaCluster())
                .thenReturn(fakeTicdcKafkaCluster());

        tidbDataSystemSourceConnectorServiceImpl.beforeConnectorCreation(tableId);

        Mockito.verify(kafkaTopicService)
                .createTICDCTopicIfAbsent(ArgumentMatchers.eq("ticdc-1-database"),
                        ArgumentMatchers.eq(fakeTicdcKafkaCluster().getId()),
                        ArgumentMatchers.eq(fakeDatabase().getId()));
    }

    private KafkaClusterDTO fakeTicdcKafkaCluster() {
        return KafkaClusterDTO.builder()
                .id(2L)
                .build();
    }

    private DataSystemResourceDTO fakeCluster() {
        return DataSystemResourceDTO.builder()
                .id(1L)
                .name("cluster")
                .build();
    }

    private DataSystemResourceDTO fakeDatabase() {
        return DataSystemResourceDTO.builder()
                .id(11L)
                .name("database")
                .build();
    }

    @Test
    public void testGenerateConnectorNameShouldAsExpect() {
        // check generated connector name as expect
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.TIDB_CLUSTER))).thenReturn(
                DataSystemResourceDTO.builder().id(1L).name("cluster").build()
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.TIDB_DATABASE))).thenReturn(
                DataSystemResourceDTO.builder().id(2L).name("database").build()
        );
        String connectorName = tidbDataSystemSourceConnectorServiceImpl.generateConnectorName(1L);
        Assertions.assertThat(connectorName).isEqualTo("source-tidb-1-database");
    }

    @Test
    public void testGenerateKafkaTopicNameShouldAsExpect() {
        // check generated topic name as expect
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.TIDB_CLUSTER))).thenReturn(
                DataSystemResourceDTO.builder().id(1L).name("cluster").build()
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.TIDB_DATABASE))).thenReturn(
                DataSystemResourceDTO.builder().id(2L).name("database").build()
        );
        when(dataSystemResourceService.getById(anyLong())).thenReturn(
                DataSystemResourceDTO.builder().id(3L).name("table").build()
        );
        String kafkaTopicName = tidbDataSystemSourceConnectorServiceImpl.generateKafkaTopicName(3L);
        Assertions.assertThat(kafkaTopicName).isEqualTo("source-tidb-1-database-table");
    }

    @Test
    public void testGenerateConnectorCustomConfigurationShouldAsExpect() {
        // check generated configuration name and value as expect
        // multiple source tidb tables
        // do mock
        List<Long> tableIds = Arrays.asList(1L, 2L, 3L);
        for (Long each : tableIds) {
            when(dataSystemResourceService.getById(eq(each))).thenReturn(DataSystemResourceDTO.builder().name("table_" + each).build());

            // table definition
            List<DataFieldDefinition> dataFieldDefinitions = new ArrayList<>();
            // table 1 has a primary key, others have unique key
            if (each == 1L) {
                dataFieldDefinitions.add(new DataFieldDefinition("table_" + each + "-field_1", "bigint", Sets.newHashSet(MysqlDataSystemConstant.Metadata.Mysql.PK_INDEX_NAME)));
            } else {
                dataFieldDefinitions.add(new DataFieldDefinition("table_" + each + "-field_1", "bigint", Sets.newHashSet("unique_key_1", "unique_key_2")));
                dataFieldDefinitions.add(new DataFieldDefinition("table_" + each + "-field_2", "bigint", Sets.newHashSet("unique_key_1", "unique_key_2")));
            }
            when(dataSystemMetadataService.getDataCollectionDefinition(eq(each))).thenReturn(new DataCollectionDefinition("table_" + each, dataFieldDefinitions));
        }
        DataSystemResourceDetailDTO cluster = generateCluster();
        when(dataSystemResourceService.getDetailParent(anyLong(), eq(DataSystemResourceType.TIDB_CLUSTER))).thenReturn(cluster);
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.TIDB_CLUSTER))).thenReturn(
                DataSystemResourceDTO.builder()
                        .id(cluster.getId())
                        .build()
        );
        when(dataSystemResourceService.getParent(anyLong(), eq(DataSystemResourceType.TIDB_DATABASE))).thenReturn(generateDatabase());
        when(dataSystemResourceService.getDetailChildren(eq(cluster.getId()),
                eq(DataSystemResourceType.TIDB_SERVER),
                eq(MysqlDataSystemResourceConfigurationDefinition.Instance.ROLE_TYPE.getName()),
                eq(MysqlInstanceRoleType.DATA_SOURCE.name())))
                .thenReturn(Arrays.asList(generateDataSourceInstance()));
        when(kafkaClusterService.getTICDCKafkaCluster()).thenReturn(
                KafkaClusterDTO.builder()
                        .bootstrapServers("6.6.6.2:6662")
                        .securityConfiguration(
                                "{\"security.protocol\":\"SASL_PLAINTEXT\","
                                        + "\"sasl.mechanism\":\"SCRAM-SHA-512\","
                                        + "\"sasl.jaas.config\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"user\\\" password=\\\"password\\\"\"}")
                        .build()
        );

        // execute
        Map<String, String> customConfiguration = tidbDataSystemSourceConnectorServiceImpl.generateConnectorCustomConfiguration(tableIds);

        // assert
        Map<String, String> desiredCustomConfiguration = new HashMap<>();
        desiredCustomConfiguration.put("database.server.name", "source-tidb-1-database");
        desiredCustomConfiguration.put("database.include", "database");
        desiredCustomConfiguration.put("table.include.list", "database.table_1,database.table_2,database.table_3");
        desiredCustomConfiguration.put("message.key.columns", "database.table_1:table_1-field_1;database.table_2:table_2-field_1,table_2-field_2;database.table_3:table_3-field_1,table_3-field_2;");
        desiredCustomConfiguration.put("source.kafka.bootstrap.servers", "6.6.6.2:6662");
        desiredCustomConfiguration.put("source.kafka.group.id", "source-tidb-1-database");
        desiredCustomConfiguration.put("source.kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"password\"");
        desiredCustomConfiguration.put("source.kafka.sasl.mechanism", "SCRAM-SHA-512");
        desiredCustomConfiguration.put("source.kafka.security.protocol", "SASL_PLAINTEXT");
        desiredCustomConfiguration.put("source.kafka.topic", "ticdc-1-database");

        Assertions.assertThat(customConfiguration).isEqualTo(desiredCustomConfiguration);
    }

    private DataSystemResourceDetailDTO generateDataSourceInstance() {
        Map<String, DataSystemResourceConfigurationDTO> dataSourceInstanceConfigurations = new HashMap<>();
        dataSourceInstanceConfigurations.put(MysqlDataSystemResourceConfigurationDefinition.Instance.HOST.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(MysqlDataSystemResourceConfigurationDefinition.Instance.HOST.getName())
                .value("6.6.6.2")
                .build());
        dataSourceInstanceConfigurations.put(MysqlDataSystemResourceConfigurationDefinition.Instance.PORT.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(MysqlDataSystemResourceConfigurationDefinition.Instance.PORT.getName())
                .value("6662")
                .build());
        dataSourceInstanceConfigurations.put(MysqlDataSystemResourceConfigurationDefinition.Instance.ROLE_TYPE.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(MysqlDataSystemResourceConfigurationDefinition.Instance.ROLE_TYPE.getName())
                .value(MysqlInstanceRoleType.DATA_SOURCE.name())
                .build());

        return new DataSystemResourceDetailDTO()
                .setId(3L)
                .setName("data_source")
                .setResourceType(DataSystemResourceType.TIDB_SERVER)
                .setDataSystemResourceConfigurations(dataSourceInstanceConfigurations);
    }

    private DataSystemResourceDTO generateDatabase() {
        return DataSystemResourceDTO.builder()
                .id(2L)
                .name("database")
                .resourceType(DataSystemResourceType.TIDB_DATABASE)
                .build();
    }

    private DataSystemResourceDetailDTO generateCluster() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap<>();
        clusterConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName())
                .value("user")
                .build());
        clusterConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName())
                .value("password")
                .build());

        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("cluster")
                .setResourceType(DataSystemResourceType.TIDB_CLUSTER)
                .setDataSystemResourceConfigurations(clusterConfigurations);
    }

    @Test
    public void testGetConnectorDefaultConfigurationShouldAsExpect() {
        // setup default configuration first
        Map<String, String> expectedDefaultConfiguration = new HashMap<>();
        expectedDefaultConfiguration.put("default-configuration-name-0", "default-configuration-value-0");
        expectedDefaultConfiguration.put("default-configuration-name-1", "default-configuration-value-1");

        Set<DefaultConnectorConfigurationDTO> defaultConnectorConfigurations = new HashSet<>();
        expectedDefaultConfiguration.forEach((key, value) -> {
            defaultConnectorConfigurations.add(
                    DefaultConnectorConfigurationDTO.builder()
                            .name(key)
                            .value(value)
                            .build()
            );
        });

        ConnectorClassDetailDTO connectorClassDetail = ConnectorClassDetailDTO.builder()
                .defaultConnectorConfigurations(defaultConnectorConfigurations)
                .build();
        when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(eq(DataSystemType.TIDB), eq(ConnectorType.SOURCE))).thenReturn(connectorClassDetail);

        Map<String, String> defaultConfiguration = tidbDataSystemSourceConnectorServiceImpl.getConnectorDefaultConfiguration();
        Assertions.assertThat(defaultConfiguration).isEqualTo(expectedDefaultConfiguration);
    }

    @Test
    public void testGetImmutableConfigurationNamesShouldAsExpect() {
        Assertions.assertThat(tidbDataSystemSourceConnectorServiceImpl.getImmutableConfigurationNames())
                .isEqualTo(Sets.newHashSet("database.server.name", "source.kafka.topic", "source.kafka.group.id"));
    }

    @Test
    public void testGetSensitiveConfigurationNamesShouldAsExpect() {
        Assertions.assertThat(tidbDataSystemSourceConnectorServiceImpl.getSensitiveConfigurationNames())
                .isEqualTo(TidbDataSystemConstant.Connector.Source.Configuration.SENSITIVE_CONFIGURATION_KEYS);
    }

    @Test
    public void testGetConnectorDataSystemResourceTypeShouldReturnDatabase() {
        Assertions.assertThat(tidbDataSystemSourceConnectorServiceImpl.getConnectorDataSystemResourceType())
                .isEqualTo(DataSystemResourceType.TIDB_DATABASE);
    }

    @Test
    public void testGetDataSystemTypeShouldReturnTidb() {
        Assertions.assertThat(tidbDataSystemSourceConnectorServiceImpl.getDataSystemType())
                .isEqualTo(DataSystemType.TIDB);
    }
}
