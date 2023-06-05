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
                    new DefaultConnectorConfigurationDTO()
                            .setName(key)
                            .setValue(value)
            );
        });
        
        ConnectorClassDetailDTO connectorClassDetail = new ConnectorClassDetailDTO()
                .setDefaultConnectorConfigurations(defaultConnectorConfigurations);
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
        
        final Map<String, String> customConfiguration = hiveDataSystemSinkConnectorServiceImpl.generateConnectorCustomConfiguration(connectionDetail.getId());
        
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
        
        return new ConnectionDetailDTO()
                .setId(1L)
                .setSourceConnectorId(2L)
                .setSinkDataCollectionId(3L)
                .setConnectionColumnConfigurations(connectionColumnConfigurations);
    }
    
    private DataSystemResourceDTO createSourceDataCollection() {
        return new DataSystemResourceDTO()
                .setId(2L)
                .setKafkaTopicName("topic_name");
    }
    
    private DataSystemResourceDTO createSinkTable() {
        return new DataSystemResourceDTO()
                .setId(3L)
                .setName("table");
    }
    
    private DataSystemResourceDTO createSinkDatabase() {
        return new DataSystemResourceDTO().setName("database");
    }
    
    private DataSystemResourceDetailDTO createHiveDetail() {
        Map<String, DataSystemResourceConfigurationDTO> hiveConfigurations = new HashMap();
        hiveConfigurations.put(Hive.HDFS_HADOOP_USER.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Hive.HDFS_HADOOP_USER.getName())
                .setValue("hive"));
        hiveConfigurations.put(Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName())
                .setValue("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"));
        hiveConfigurations.put(Hive.HIVE_METASTORE_URIS.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Hive.HIVE_METASTORE_URIS.getName())
                .setValue("thrift://localhost:9803"));
        
        hiveConfigurations.put(Hive.HDFS_NAME_SERVICES.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Hive.HDFS_NAME_SERVICES.getName())
                .setValue("testCluster"));
        
        hiveConfigurations.put(Hive.HDFS_NAME_NODES.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Hive.HDFS_NAME_NODES.getName())
                .setValue("nn1=name-node-01:8020,nn2=name-node-02:8020"));
        
        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("hive")
                .setResourceType(DataSystemResourceType.HIVE)
                .setDataSystemResourceConfigurations(hiveConfigurations);
    }
    
}
