package cn.xdf.acdc.devops.service.process.datasystem.hive;

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
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hdfs;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hive;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class HiveDataSystemSinkConnectorServiceTest {

    private HiveDataSystemSinkConnectorServiceImpl hiveDataSystemSinkConnectorServiceImpl;

    @Mock
    private ConnectorClassService mockConnectorClassService;

    @Mock
    private DataSystemResourceService mockDataSystemResourceService;

    @Mock
    private ConnectionService mockConnectionService;

    @Before
    public void setUp() throws Exception {
        hiveDataSystemSinkConnectorServiceImpl = new HiveDataSystemSinkConnectorServiceImpl();
        ReflectionTestUtils.setField(hiveDataSystemSinkConnectorServiceImpl, "connectionService", mockConnectionService);
        ReflectionTestUtils.setField(hiveDataSystemSinkConnectorServiceImpl, "dataSystemResourceService", mockDataSystemResourceService);
        ReflectionTestUtils.setField(hiveDataSystemSinkConnectorServiceImpl, "connectorClassService", mockConnectorClassService);
    }

    @Test
    public void testVerifyDataSystemMetadata() {
        hiveDataSystemSinkConnectorServiceImpl.verifyDataSystemMetadata(1L);
    }

    @Test
    public void testBeforeConnectorCreation() {
        hiveDataSystemSinkConnectorServiceImpl.beforeConnectorCreation(1L);
    }

    @Test
    public void testGetConnectorDefaultConfiguration() {
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
        when(mockConnectorClassService.getDetailByDataSystemTypeAndConnectorType(eq(DataSystemType.HIVE), eq(ConnectorType.SINK))).thenReturn(connectorClassDetail);

        Map<String, String> defaultConfiguration = hiveDataSystemSinkConnectorServiceImpl.getConnectorDefaultConfiguration();
        Assertions.assertThat(defaultConfiguration).isEqualTo(expectedDefaultConfiguration);
    }

    @Test
    public void testGenerateConnectorCustomConfiguration() {
        ConnectionDetailDTO connectionDetail = createConnectionDetail();
        DataSystemResourceDTO sourceDataCollection = createSourceDataCollection();
        DataSystemResourceDTO sinkTable = createSinkTable();
        DataSystemResourceDTO sinkDatabase = createSinkDatabase();
        DataSystemResourceDetailDTO hiveDetail = createHiveDetail();
        DataSystemResourceDTO hive = new DataSystemResourceDTO()
                .setName(hiveDetail.getName())
                .setId(hiveDetail.getId());

        when(mockConnectionService.getDetailById(eq(connectionDetail.getId()))).thenReturn(connectionDetail);
        when(mockDataSystemResourceService.getById(eq(connectionDetail.getSourceDataCollectionId()))).thenReturn(sourceDataCollection);
        when(mockDataSystemResourceService.getById(eq(connectionDetail.getSinkDataCollectionId()))).thenReturn(sinkTable);
        when(mockDataSystemResourceService.getParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.HIVE_DATABASE))).thenReturn(sinkDatabase);
        when(mockDataSystemResourceService.getDetailParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.HIVE))).thenReturn(hiveDetail);
        when(mockDataSystemResourceService.getParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.HIVE))).thenReturn(hive);

        Map<String, String> customConfiguration = hiveDataSystemSinkConnectorServiceImpl.generateConnectorCustomConfiguration(connectionDetail.getId());

        // assertion
        Map<String, String> desiredCustomConfiguration = new HashMap<>();
//        desiredCustomConfiguration.put("name", "sink-hive-1-database-table");
        desiredCustomConfiguration.put("topics", "topic_name");
        desiredCustomConfiguration.put("destinations", "database.table");

        desiredCustomConfiguration.put("destinations.database.table.fields.whitelist", "source_column_name");
        desiredCustomConfiguration.put("destinations.database.table.fields.mapping", "source_column_name:sink_column_name");
        desiredCustomConfiguration.put("destinations.database.table.delete.mode", "NONE");

        // hdfs and hive
        desiredCustomConfiguration.put("hadoop.user", "hive");
        desiredCustomConfiguration.put("store.url", "hdfs://testCluster");
        desiredCustomConfiguration.put("__hdfs.dfs.nameservices", "testCluster");
        desiredCustomConfiguration.put("__hdfs.dfs.client.failover.proxy.provider.testCluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        desiredCustomConfiguration.put("__hdfs.dfs.ha.namenodes.testCluster", "nn1,nn2");
        desiredCustomConfiguration.put("__hdfs.dfs.namenode.rpc-address.testCluster.nn1", "name-node-01:8020");
        desiredCustomConfiguration.put("__hdfs.dfs.namenode.rpc-address.testCluster.nn2", "name-node-02:8020");
        desiredCustomConfiguration.put("hive.metastore.uris", "thrift://localhost:9803");

        Assertions.assertThat(customConfiguration).isEqualTo(desiredCustomConfiguration);
    }

    @Test
    public void testGetConnectorSpecificConfigurationDefinitionsShouldReturnEmptyList() {
        Assertions.assertThat(hiveDataSystemSinkConnectorServiceImpl.getConnectorSpecificConfigurationDefinitions()).isEmpty();
    }

    @Test
    public void testGetSensitiveConfigurationNamesShouldReturnEmptySet() {
        Assertions.assertThat(hiveDataSystemSinkConnectorServiceImpl.getSensitiveConfigurationNames()).isEmpty();
    }

    @Test
    public void testGetDataSystemTypeShouldReturnHive() {
        Assertions.assertThat(hiveDataSystemSinkConnectorServiceImpl.getDataSystemType()).isEqualTo(DataSystemType.HIVE);
    }

    private ConnectionDetailDTO createConnectionDetail() {
        List<ConnectionColumnConfigurationDTO> connectionColumnConfigurations = new ArrayList<>();
        connectionColumnConfigurations.add(new ConnectionColumnConfigurationDTO()
                .setSourceColumnName("source_column_name")
                .setSinkColumnName("sink_column_name")
        );

        return ConnectionDetailDTO.builder()
                .id(1L)
                .sourceConnectorId(2L)
                .sinkDataCollectionId(3L)
                .connectionColumnConfigurations(connectionColumnConfigurations)
                .build();
    }

    private DataSystemResourceDTO createSourceDataCollection() {
        return DataSystemResourceDTO.builder()
                .id(2L)
                .kafkaTopicName("topic_name")
                .build();
    }

    private DataSystemResourceDTO createSinkTable() {
        return DataSystemResourceDTO.builder()
                .id(3L)
                .name("table")
                .build();
    }

    private DataSystemResourceDTO createSinkDatabase() {
        return DataSystemResourceDTO.builder().name("database").build();
    }

    private DataSystemResourceDetailDTO createHiveDetail() {
        Map<String, DataSystemResourceConfigurationDTO> hiveConfigurations = new HashMap();
        hiveConfigurations.put(Hdfs.HDFS_HADOOP_USER.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Hdfs.HDFS_HADOOP_USER.getName())
                .value("hive")
                .build());
        hiveConfigurations.put(Hdfs.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Hdfs.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName())
                .value("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
                .build());
        hiveConfigurations.put(Hive.HIVE_METASTORE_URIS.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Hive.HIVE_METASTORE_URIS.getName())
                .value("thrift://localhost:9803")
                .build());

        hiveConfigurations.put(Hdfs.HDFS_NAME_SERVICES.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Hdfs.HDFS_NAME_SERVICES.getName())
                .value("testCluster")
                .build());

        hiveConfigurations.put(Hdfs.HDFS_NAME_NODES.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Hdfs.HDFS_NAME_NODES.getName())
                .value("nn1=name-node-01:8020,nn2=name-node-02:8020")
                .build());

        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("hive")
                .setResourceType(DataSystemResourceType.HIVE)
                .setDataSystemResourceConfigurations(hiveConfigurations);
    }

}
