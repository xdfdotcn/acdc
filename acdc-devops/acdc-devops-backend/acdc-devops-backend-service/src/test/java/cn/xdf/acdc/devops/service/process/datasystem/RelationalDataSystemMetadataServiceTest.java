package cn.xdf.acdc.devops.service.process.datasystem;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
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

import com.google.common.collect.Sets;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinitionExtendPropertyName;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlInstanceRoleType;
import cn.xdf.acdc.devops.service.process.datasystem.relational.RelationalDataSystemMetadataService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.RelationalDatabaseTableField;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RelationalDataSystemMetadataServiceTest {
    
    @Autowired
    @Qualifier("mysqlDataSystemMetadataServiceImpl")
    private RelationalDataSystemMetadataService dataSystemMetadataService;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private MysqlHelperService mysqlHelperService;
    
    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testGetDataCollectionDefinitionShouldAsExpect() {
        DataSystemResourceDTO table = new DataSystemResourceDTO().setId(1L).setName("table_1");
        
        when(dataSystemResourceService.getById(eq(table.getId()))).thenReturn(table);
        when(dataSystemResourceService.getDetailParent(eq(table.getId()), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(generateCluster());
        when(dataSystemResourceService.getParent(eq(table.getId()), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(new DataSystemResourceDTO().setName("database_1"));
        when(dataSystemResourceService.getDetailChildren(eq(table.getId()), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(generateInstances());
        
        List<RelationalDatabaseTableField> dataFieldDefinitions = new ArrayList<>();
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_1", "bigint", Sets.newHashSet("PRIMARY")));
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_2", "varchar(32)", Sets.newHashSet("unique_index_1")));
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_3", "varchar(128)", Sets.newHashSet("unique_index_2", "multi_unique_index_1")));
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_4", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));
        
        when(mysqlHelperService.descTable(anySet(), any(), anyString(), anyString())).thenReturn(dataFieldDefinitions);
        
        final DataCollectionDefinition dataCollectionDefinition = dataSystemMetadataService.getDataCollectionDefinition(table.getId());
        
        List<DataFieldDefinition> expectedDataFieldDefinitions = new ArrayList<>();
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_1", "bigint", Schema.INT64_SCHEMA, false, null, new HashMap<>(), Sets.newHashSet("PRIMARY")));
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_2", "varchar(32)", Schema.STRING_SCHEMA, false,
                null,
                getLengthProperty(32),
                Sets.newHashSet("unique_index_1")));
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_3",
                "varchar(128)",
                Schema.STRING_SCHEMA,
                false,
                null,
                getLengthProperty(128),
                Sets.newHashSet("unique_index_2",
                        "multi_unique_index_1")));
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_4",
                "varchar(128)",
                Schema.STRING_SCHEMA,
                false,
                null,
                getLengthProperty(128),
                Sets.newHashSet("multi_unique_index_1")));
        
        DataCollectionDefinition expectedDataCollectionDefinition = new DataCollectionDefinition("table_1", expectedDataFieldDefinitions);
        
        Assertions.assertThat(dataCollectionDefinition).isEqualTo(expectedDataCollectionDefinition);
    }
    
    private Map<DataFieldDefinitionExtendPropertyName, Object> getLengthProperty(final Integer value) {
        Map<DataFieldDefinitionExtendPropertyName, Object> result = new HashMap<>();
        result.put(DataFieldDefinitionExtendPropertyName.LENGTH, value);
        return result;
    }
    
    @Test
    public void testRefreshDynamicDataSystemResourceShouldRefreshDatabaseAndTable() {
        DataSystemResourceDetailDTO cluster = generateCluster();
        
        List<String> databases = Arrays.asList("database_1", "database_2", "database_3");
        when(mysqlHelperService.showDataBases(anySet(), any(), any())).thenReturn(databases);
        List<String> tables = Arrays.asList("table_1", "table_2", "table_3");
        when(mysqlHelperService.showTables(anySet(), any(), anyString())).thenReturn(tables);
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(generateInstances());
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(generateDatabases());
        
        dataSystemMetadataService.refreshDynamicDataSystemResource(cluster.getId());
        
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> captor = ArgumentCaptor.forClass(List.class);
        
        // 1 time for database, 2 times for table
        Mockito.verify(dataSystemResourceService, times(3)).mergeAllChildrenByName(captor.capture(), any(), anyLong());
        
        Assertions.assertThat(captor.getAllValues().get(0).size()).isEqualTo(databases.size());
        captor.getAllValues().get(0).forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_DATABASE);
            Assertions.assertThat(databases.contains(each.getName()));
        });
        
        Assertions.assertThat(captor.getAllValues().get(1).size()).isEqualTo(tables.size());
        captor.getAllValues().get(1).forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_TABLE);
            Assertions.assertThat(tables.contains(each.getName()));
        });
        
        Assertions.assertThat(captor.getAllValues().get(2).size()).isEqualTo(tables.size());
        captor.getAllValues().get(2).forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_TABLE);
            Assertions.assertThat(tables.contains(each.getName()));
        });
    }
    
    @Test
    public void testRefreshDynamicDataSystemResourceShouldSkipDatabaseIfSourceIsPandora() {
        DataSystemResourceDetailDTO cluster = generateCluster();
        
        // add source configuration
        cluster.getDataSystemResourceConfigurations().put(Cluster.SOURCE.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Cluster.SOURCE.getName())
                .setValue(MetadataSourceType.FROM_PANDORA.name()));
        
        List<DataSystemResourceDetailDTO> instances = generateInstances();
        
        List<String> databases = Arrays.asList("database_1", "database_2", "database_3");
        when(mysqlHelperService.showDataBases(anySet(), any(), any())).thenReturn(databases);
        List<String> tables = Arrays.asList("table_1", "table_2", "table_3");
        when(mysqlHelperService.showTables(anySet(), any(), anyString())).thenReturn(tables);
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instances);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(generateDatabases());
        
        dataSystemMetadataService.refreshDynamicDataSystemResource(cluster.getId());
        
        // 2 times for table
        Mockito.verify(dataSystemResourceService, Mockito.times(2)).mergeAllChildrenByName(anyList(), any(), anyLong());
    }
    
    @Test
    public void testRefreshDynamicDataSystemResourceShouldUseParentsProjectRelation() {
        DataSystemResourceDetailDTO cluster = generateCluster();
        List<ProjectDTO> projects = generateProjects();
        cluster.setProjects(projects);
        
        List<String> databases = Arrays.asList("database_1", "database_2", "database_3");
        when(mysqlHelperService.showDataBases(anySet(), any(), any())).thenReturn(databases);
        List<String> tables = Arrays.asList("table_1", "table_2", "table_3");
        when(mysqlHelperService.showTables(anySet(), any(), anyString())).thenReturn(tables);
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(generateInstances());
        
        List<DataSystemResourceDetailDTO> databaseResources = generateDatabases();
        databaseResources.forEach(each -> each.setProjects(projects));
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(databaseResources);
        
        dataSystemMetadataService.refreshDynamicDataSystemResource(cluster.getId());
        
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> captor = ArgumentCaptor.forClass(List.class);
        
        // 1 time for database, 2 times for table
        Mockito.verify(dataSystemResourceService, times(3)).mergeAllChildrenByName(captor.capture(), any(), anyLong());
        
        Assertions.assertThat(captor.getAllValues().get(0).size()).isEqualTo(databases.size());
        captor.getAllValues().get(0).forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_DATABASE);
            Assertions.assertThat(each.getProjects()).isEqualTo(projects);
        });
        
        Assertions.assertThat(captor.getAllValues().get(1).size()).isEqualTo(tables.size());
        captor.getAllValues().get(1).forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_TABLE);
            Assertions.assertThat(each.getProjects()).isEqualTo(projects);
        });
        
        Assertions.assertThat(captor.getAllValues().get(2).size()).isEqualTo(tables.size());
        captor.getAllValues().get(2).forEach(each -> {
            Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_TABLE);
            Assertions.assertThat(each.getProjects()).isEqualTo(projects);
        });
    }
    
    private DataSystemResourceDetailDTO generateCluster() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap();
        clusterConfigurations.put(Cluster.USERNAME.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Cluster.USERNAME.getName())
                .setValue("username"));
        clusterConfigurations.put(Cluster.PASSWORD.getName(), new DataSystemResourceConfigurationDTO()
                .setName(Cluster.PASSWORD.getName())
                .setValue(EncryptUtil.encrypt("password")));
        
        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("cluster")
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemResourceConfigurations(clusterConfigurations);
    }
    
    private List<ProjectDTO> generateProjects() {
        List<ProjectDTO> projects = new ArrayList<>();
        
        for (int i = 0; i < 3; i++) {
            projects.add(
                    new ProjectDTO()
                            .setId(Long.valueOf(i))
                            .setName("project_" + i)
            );
        }
        
        return projects;
    }
    
    private List<DataSystemResourceDetailDTO> generateInstances() {
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
        
        List<DataSystemResourceDetailDTO> instanceDetails = new ArrayList();
        instanceDetails.add(new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("data_source")
                .setResourceType(DataSystemResourceType.MYSQL_INSTANCE)
                .setDataSystemResourceConfigurations(dataSourceInstanceConfigurations));
        instanceDetails.add(new DataSystemResourceDetailDTO()
                .setId(2L)
                .setName("master")
                .setResourceType(DataSystemResourceType.MYSQL_INSTANCE)
                .setDataSystemResourceConfigurations(masterInstanceConfigurations));
        
        return instanceDetails;
    }
    
    private List<DataSystemResourceDetailDTO> generateDatabases() {
        List<DataSystemResourceDetailDTO> instanceDetails = new ArrayList();
        
        instanceDetails.add(
                new DataSystemResourceDetailDTO()
                        .setId(1L)
                        .setName("old_database_1")
                        .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
        );
        instanceDetails.add(
                new DataSystemResourceDetailDTO()
                        .setId(2L)
                        .setName("old_database_2")
                        .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
        );
        
        return instanceDetails;
    }
}
