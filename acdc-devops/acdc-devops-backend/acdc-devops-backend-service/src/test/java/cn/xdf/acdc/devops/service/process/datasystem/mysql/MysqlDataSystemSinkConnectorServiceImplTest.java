package cn.xdf.acdc.devops.service.process.datasystem.mysql;

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
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connector.ConnectorClassService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemSinkConnectorService;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.util.UrlUtil;
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
import org.testcontainers.shaded.com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
public class MysqlDataSystemSinkConnectorServiceImplTest {
    
    @Autowired
    @Qualifier("mysqlDataSystemSinkConnectorServiceImpl")
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
    public void testVerifyDataSystemMetadataShouldPass() {
        when(dataSystemResourceService.getDetailParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDetailDTO().setId(1L)
        );
        
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE), eq(Instance.ROLE_TYPE.getName()), eq(MysqlInstanceRoleType.MASTER.name())))
                .thenReturn(Arrays.asList(new DataSystemResourceDetailDTO()));
        
        dataSystemSinkConnectorService.verifyDataSystemMetadata(1L);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testVerifyDataSystemMetadataShouldErrorWhenThereIsNoMasterInstance() {
        when(dataSystemResourceService.getDetailParent(anyLong(), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(
                new DataSystemResourceDetailDTO().setId(1L)
        );
        
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE), eq(Instance.ROLE_TYPE.getName()), eq(MysqlInstanceRoleType.MASTER.name())))
                .thenReturn(Collections.emptyList());
        
        dataSystemSinkConnectorService.verifyDataSystemMetadata(1L);
    }
    
    @Test
    public void testBeforeConnectorCreationShouldPass() {
        dataSystemSinkConnectorService.beforeConnectorCreation(1L);
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
        when(connectorClassService.getDetailByDataSystemTypeAndConnectorType(eq(DataSystemType.MYSQL), eq(ConnectorType.SINK))).thenReturn(connectorClassDetail);
        
        Map<String, String> defaultConfiguration = dataSystemSinkConnectorService.getConnectorDefaultConfiguration();
        Assertions.assertThat(defaultConfiguration).isEqualTo(expectedDefaultConfiguration);
    }
    
    @Test
    public void testGenerateConnectorCustomConfigurationShouldAsExpect() {
        // just mock one column configuration here, more case should be test in AbstractDataSystemSinkConnectorServiceTest
        ConnectionDetailDTO connectionDetail = generateConnectionDetail();
        when(connectionService.getDetailById(eq(connectionDetail.getId()))).thenReturn(connectionDetail);
        
        DataSystemResourceDTO sourceDataCollection = generateSourceDataCollection();
        when(dataSystemResourceService.getById(eq(connectionDetail.getSourceDataCollectionId()))).thenReturn(sourceDataCollection);
        
        DataSystemResourceDTO sinkTable = generateSinkTable();
        when(dataSystemResourceService.getById(eq(connectionDetail.getSinkDataCollectionId()))).thenReturn(sinkTable);
        
        DataSystemResourceDetailDTO clusterDetail = generateClusterDetail();
        when(dataSystemResourceService.getDetailParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(clusterDetail);
        
        DataSystemResourceDTO database = generateDatabase();
        when(dataSystemResourceService.getParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(database);
        
        DataSystemResourceDetailDTO masterInstanceDetail = generateMasterInstanceDetail();
        when(dataSystemResourceService
                .getDetailChildren(eq(clusterDetail.getId()), eq(DataSystemResourceType.MYSQL_INSTANCE), eq(Instance.ROLE_TYPE.getName()), eq(MysqlInstanceRoleType.MASTER.name())))
                .thenReturn(Arrays.asList(masterInstanceDetail));
        
        // execute
        final Map<String, String> customConfiguration = dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionDetail.getId());
        
        // assertion
        Map<String, String> desiredCustomConfiguration = new HashMap<>();
        desiredCustomConfiguration.put("topics", sourceDataCollection.getKafkaTopicName());
        desiredCustomConfiguration.put("destinations", sinkTable.getName());
        
        String host = masterInstanceDetail.getDataSystemResourceConfigurations().get(Instance.HOST.getName()).getValue();
        int port = Integer.parseInt(masterInstanceDetail.getDataSystemResourceConfigurations().get(Instance.PORT.getName()).getValue());
        desiredCustomConfiguration.put("connection.url", UrlUtil.generateJDBCUrl(DataSystemType.MYSQL.name().toLowerCase(), host, port, database.getName()));
        
        desiredCustomConfiguration.put("connection.user", clusterDetail.getDataSystemResourceConfigurations().get(Cluster.USERNAME.getName()).getValue());
        desiredCustomConfiguration.put("connection.password", clusterDetail.getDataSystemResourceConfigurations().get(Cluster.PASSWORD.getName()).getValue());
        
        desiredCustomConfiguration.put("destinations." + sinkTable.getName() + ".fields.whitelist", "source_column_name");
        desiredCustomConfiguration.put("destinations." + sinkTable.getName() + ".fields.mapping", "source_column_name:sink_column_name");
        
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
                .setConnectionColumnConfigurations(connectionColumnConfigurations);
    }
    
    private DataSystemResourceDTO generateSourceDataCollection() {
        return new DataSystemResourceDTO()
                .setId(2L)
                .setKafkaTopicName("topic_name");
    }
    
    private DataSystemResourceDTO generateSinkTable() {
        return new DataSystemResourceDTO()
                .setId(3L)
                .setName("sink_table");
    }
    
    private DataSystemResourceDetailDTO generateClusterDetail() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap();
        clusterConfigurations.put(Cluster.USERNAME.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Cluster.USERNAME.getName())
                .setValue("username"));
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
                .setName("database");
    }
    
    private DataSystemResourceDetailDTO generateMasterInstanceDetail() {
        Map<String, DataSystemResourceConfigurationDTO> masterInstanceConfigurations = new HashMap();
        masterInstanceConfigurations.put(Instance.HOST.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Instance.HOST.getName())
                .setValue("6.6.6.2"));
        masterInstanceConfigurations.put(Instance.PORT.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Instance.PORT.getName())
                .setValue("6662"));
        masterInstanceConfigurations.put(Instance.ROLE_TYPE.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Instance.ROLE_TYPE.getName())
                .setValue(MysqlInstanceRoleType.MASTER.name()));
        
        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("master")
                .setResourceType(DataSystemResourceType.MYSQL_INSTANCE)
                .setDataSystemResourceConfigurations(masterInstanceConfigurations);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testGenerateConnectorCustomConfigurationShouldErrorWhenNoMasterInstance() {
        ConnectionDetailDTO connectionDetail = generateConnectionDetail();
        when(connectionService.getDetailById(eq(connectionDetail.getId()))).thenReturn(connectionDetail);
        
        DataSystemResourceDTO sourceDataCollection = generateSourceDataCollection();
        when(dataSystemResourceService.getById(eq(connectionDetail.getSourceDataCollectionId()))).thenReturn(sourceDataCollection);
        
        DataSystemResourceDTO sinkTable = generateSinkTable();
        when(dataSystemResourceService.getById(eq(connectionDetail.getSinkDataCollectionId()))).thenReturn(sinkTable);
        
        DataSystemResourceDetailDTO clusterDetail = generateClusterDetail();
        when(dataSystemResourceService.getDetailParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(clusterDetail);
        
        DataSystemResourceDTO database = generateDatabase();
        when(dataSystemResourceService.getParent(eq(connectionDetail.getSinkDataCollectionId()), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(database);
        
        when(dataSystemResourceService
                .getDetailChildren(eq(clusterDetail.getId()), eq(DataSystemResourceType.MYSQL_INSTANCE), eq(Instance.ROLE_TYPE.getName()), eq(MysqlInstanceRoleType.MASTER.name())))
                .thenReturn(Collections.emptyList());
        
        dataSystemSinkConnectorService.generateConnectorCustomConfiguration(connectionDetail.getId());
    }
    
    @Test
    public void testGetConnectorSpecificConfigurationDefinitionsShouldReturnEmptyList() {
        Assertions.assertThat(dataSystemSinkConnectorService.getConnectorSpecificConfigurationDefinitions()).isEmpty();
    }
    
    @Test
    public void testGetSensitiveConfigurationNamesShouldAsExcept() {
        Assertions.assertThat(dataSystemSinkConnectorService.getSensitiveConfigurationNames())
                .isEqualTo(Sets.newHashSet("connection.password"));
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnMysql() {
        Assertions.assertThat(dataSystemSinkConnectorService.getDataSystemType()).isEqualTo(DataSystemType.MYSQL);
    }
    
    @Test
    public void testGetConnectorClassShouldAsExpect() {
        dataSystemSinkConnectorService.getConnectorClass();
        
        ArgumentCaptor<DataSystemType> dataSystemTypeCaptor = ArgumentCaptor.forClass(DataSystemType.class);
        ArgumentCaptor<ConnectorType> connectorTypeCaptor = ArgumentCaptor.forClass(ConnectorType.class);
        
        Mockito.verify(connectorClassService).getDetailByDataSystemTypeAndConnectorType(dataSystemTypeCaptor.capture(), connectorTypeCaptor.capture());
        Assertions.assertThat(dataSystemTypeCaptor.getValue()).isEqualTo(DataSystemType.MYSQL);
        Assertions.assertThat(connectorTypeCaptor.getValue()).isEqualTo(ConnectorType.SINK);
    }
}
