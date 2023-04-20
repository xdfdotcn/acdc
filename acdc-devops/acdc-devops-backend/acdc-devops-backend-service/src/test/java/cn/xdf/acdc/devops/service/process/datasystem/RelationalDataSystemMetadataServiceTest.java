package cn.xdf.acdc.devops.service.process.datasystem;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlInstanceRoleType;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.RelationalDatabaseTableField;
import com.google.common.collect.Sets;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

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
        DataSystemResourceDTO table = DataSystemResourceDTO.builder().id(1L).name("table_1").build();

        when(dataSystemResourceService.getById(eq(table.getId()))).thenReturn(table);
        when(dataSystemResourceService.getDetailParent(eq(table.getId()), eq(DataSystemResourceType.MYSQL_CLUSTER))).thenReturn(generateCluster());
        when(dataSystemResourceService.getParent(eq(table.getId()), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(generateDatabases().get(0));
        when(dataSystemResourceService.getDetailChildren(eq(table.getId()), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(generateInstances());

        List<RelationalDatabaseTableField> dataFieldDefinitions = new ArrayList<>();
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_1", "bigint", Sets.newHashSet("PRIMARY")));
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_2", "varchar(32)", Sets.newHashSet("unique_index_1")));
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_3", "varchar(128)", Sets.newHashSet("unique_index_2", "multi_unique_index_1")));
        dataFieldDefinitions.add(new RelationalDatabaseTableField("column_4", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));

        when(mysqlHelperService.descTable(anySet(), any(), anyString(), anyString())).thenReturn(dataFieldDefinitions);

        DataCollectionDefinition dataCollectionDefinition = dataSystemMetadataService.getDataCollectionDefinition(table.getId());

        List<DataFieldDefinition> expectedDataFieldDefinitions = new ArrayList<>();
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_1", "bigint", Sets.newHashSet("PRIMARY")));
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_2", "varchar(32)", Sets.newHashSet("unique_index_1")));
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_3", "varchar(128)", Sets.newHashSet("unique_index_2", "multi_unique_index_1")));
        expectedDataFieldDefinitions.add(new DataFieldDefinition("column_4", "varchar(128)", Sets.newHashSet("multi_unique_index_1")));

        DataCollectionDefinition expectedDataCollectionDefinition = new DataCollectionDefinition("table_1", expectedDataFieldDefinitions);

        Assertions.assertThat(dataCollectionDefinition).isEqualTo(expectedDataCollectionDefinition);
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
        when(dataSystemResourceService.getChildren(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(generateDatabases());

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

    private DataSystemResourceDetailDTO generateCluster() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap();
        clusterConfigurations.put(Cluster.USERNAME.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.USERNAME.getName())
                .value("username")
                .build());
        clusterConfigurations.put(Cluster.PASSWORD.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.PASSWORD.getName())
                .value(EncryptUtil.encrypt("password"))
                .build());

        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("cluster")
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemResourceConfigurations(clusterConfigurations);
    }

    private List<DataSystemResourceDetailDTO> generateInstances() {
        Map<String, DataSystemResourceConfigurationDTO> dataSourceInstanceConfigurations = new HashMap();
        dataSourceInstanceConfigurations.put(Instance.HOST.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Instance.HOST.getName())
                .value("6.6.6.2")
                .build());
        dataSourceInstanceConfigurations.put(Instance.PORT.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Instance.PORT.getName())
                .value("6662")
                .build());
        dataSourceInstanceConfigurations.put(Instance.ROLE_TYPE.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Instance.ROLE_TYPE.getName())
                .value(MysqlInstanceRoleType.DATA_SOURCE.name())
                .build());

        Map<String, DataSystemResourceConfigurationDTO> masterInstanceConfigurations = new HashMap();
        masterInstanceConfigurations.put(Instance.HOST.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Instance.HOST.getName())
                .value("6.6.6.2")
                .build());
        masterInstanceConfigurations.put(Instance.PORT.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Instance.PORT.getName())
                .value("6662")
                .build());
        masterInstanceConfigurations.put(Instance.ROLE_TYPE.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Instance.ROLE_TYPE.getName())
                .value(MysqlInstanceRoleType.MASTER.name())
                .build());

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

    private List<DataSystemResourceDTO> generateDatabases() {
        List<DataSystemResourceDTO> instanceDetails = new ArrayList();

        instanceDetails.add(DataSystemResourceDTO.builder()
                .id(1L)
                .name("old_database_1")
                .resourceType(DataSystemResourceType.MYSQL_DATABASE)
                .build());
        instanceDetails.add(DataSystemResourceDTO.builder()
                .id(2L)
                .name("old_database_2")
                .resourceType(DataSystemResourceType.MYSQL_DATABASE)
                .build());

        return instanceDetails;
    }

    @Test
    public void testRefreshDynamicDataSystemResourceShouldSkipDatabaseIfSourceIsPandora() {
        DataSystemResourceDetailDTO cluster = generateCluster();

        // add source configuration
        cluster.getDataSystemResourceConfigurations().put(Cluster.SOURCE.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(Cluster.SOURCE.getName())
                .value(MetadataSourceType.FROM_PANDORA.name())
                .build());

        List<DataSystemResourceDetailDTO> instances = generateInstances();

        List<String> databases = Arrays.asList("database_1", "database_2", "database_3");
        when(mysqlHelperService.showDataBases(anySet(), any(), any())).thenReturn(databases);
        List<String> tables = Arrays.asList("table_1", "table_2", "table_3");
        when(mysqlHelperService.showTables(anySet(), any(), anyString())).thenReturn(tables);

        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instances);
        when(dataSystemResourceService.getChildren(anyLong(), eq(DataSystemResourceType.MYSQL_DATABASE))).thenReturn(generateDatabases());

        dataSystemMetadataService.refreshDynamicDataSystemResource(cluster.getId());

        // 2 times for table
        Mockito.verify(dataSystemResourceService, Mockito.times(2)).mergeAllChildrenByName(anyList(), any(), anyLong());
    }
}
