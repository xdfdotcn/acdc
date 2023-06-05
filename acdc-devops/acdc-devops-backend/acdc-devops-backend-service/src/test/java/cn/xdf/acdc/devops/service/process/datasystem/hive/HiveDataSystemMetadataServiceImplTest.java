package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ProjectDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataCollectionDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hive;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.HiveHelperService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.RelationalDatabaseTableField;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class HiveDataSystemMetadataServiceImplTest {
    
    @Mock
    private HiveHelperService mockHiveHelperService;
    
    @Mock
    private DataSystemResourceService mockDataSystemResourceService;
    
    @Mock
    private I18nService mockI18n;
    
    private HiveDataSystemMetadataServiceImpl hiveDataSystemMetadataServiceImpl;
    
    @Before
    public void setUp() throws Exception {
        hiveDataSystemMetadataServiceImpl = new HiveDataSystemMetadataServiceImpl();
        ReflectionTestUtils.setField(hiveDataSystemMetadataServiceImpl, "hiveHelperService", mockHiveHelperService);
        ReflectionTestUtils.setField(hiveDataSystemMetadataServiceImpl, "dataSystemResourceService", mockDataSystemResourceService);
        ReflectionTestUtils.setField(hiveDataSystemMetadataServiceImpl, "i18n", mockI18n);
    }
    
    @Test
    public void testCheckDataSystem() {
        when(mockDataSystemResourceService.getDetailById(any())).thenReturn(createHiveDetail());
        hiveDataSystemMetadataServiceImpl.checkDataSystem(1L);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldThrowExceptionWhenMissingConfiguration() {
        DataSystemResourceDetailDTO hiveResourceDetailDTO = createHiveDetail();
        hiveResourceDetailDTO.getDataSystemResourceConfigurations().clear();
        when(mockDataSystemResourceService.getDetailById(any())).thenReturn(hiveResourceDetailDTO);
        hiveDataSystemMetadataServiceImpl.checkDataSystem(1L);
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnHive() {
        Assertions.assertThat(hiveDataSystemMetadataServiceImpl.getDataSystemType()).isEqualTo(DataSystemType.HIVE);
    }
    
    @Test
    public void testRefreshDatabases() {
        DataSystemResourceDetailDTO hiveDetail = createHiveDetail();
        List<String> hiveDatabases = createHiveDatabaseNameLimit2();
        final Map<String, String> hiveDatabaseMap = hiveDatabases.stream().collect(Collectors.toMap(it -> it, it -> it));
        
        when(mockHiveHelperService.showDatabases()).thenReturn(hiveDatabases);
        
        hiveDataSystemMetadataServiceImpl.refreshDatabases(hiveDetail);
        
        // verify
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> dataSystemResourceDetailDTOListCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockDataSystemResourceService, Mockito.times(1))
                .mergeAllChildrenByName(dataSystemResourceDetailDTOListCaptor.capture(), eq(DataSystemResourceType.HIVE_DATABASE), eq(hiveDetail.getId()));
        
        dataSystemResourceDetailDTOListCaptor.getValue().forEach(it -> {
            Assertions.assertThat(hiveDatabaseMap.get(it.getName())).isEqualTo(it.getName());
            Assertions.assertThat(it.getResourceType()).isEqualTo(DataSystemResourceType.HIVE_DATABASE);
            Assertions.assertThat(it.getParentResource().getId()).isEqualTo(hiveDetail.getId());
        });
    }
    
    @Test
    public void testRefreshTables() {
        DataSystemResourceDetailDTO hiveDetail = createHiveDetail();
        List<String> hiveTables = createHiveTableNameLimit2();
        DataSystemResourceDetailDTO hiveDatabaseDetailDTO = createHiveDatabaseResourceDetailDTO();
        final Map<String, String> hiveTableMap = hiveTables.stream().collect(Collectors.toMap(it -> it, it -> it));
        
        when(mockDataSystemResourceService.getDetailChildren(eq(hiveDetail.getId()), eq(DataSystemResourceType.HIVE_DATABASE))).thenReturn(Lists.newArrayList(hiveDatabaseDetailDTO));
        when(mockHiveHelperService.showTables(any())).thenReturn(hiveTables);
        
        hiveDataSystemMetadataServiceImpl.refreshTables(hiveDetail);
        
        // verify
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> dataSystemResourceDetailDTOListCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockDataSystemResourceService, Mockito.times(1))
                .mergeAllChildrenByName(dataSystemResourceDetailDTOListCaptor.capture(), eq(DataSystemResourceType.HIVE_TABLE), eq(hiveDatabaseDetailDTO.getId()));
        
        dataSystemResourceDetailDTOListCaptor.getValue().forEach(it -> {
            Assertions.assertThat(hiveTableMap.get(it.getName())).isEqualTo(it.getName());
            Assertions.assertThat(it.getResourceType()).isEqualTo(DataSystemResourceType.HIVE_TABLE);
            Assertions.assertThat(it.getParentResource().getId()).isEqualTo(hiveDatabaseDetailDTO.getId());
        });
    }
    
    @Test
    public void testRefreshTablesOfDatabase() {
        List<String> hiveTables = createHiveTableNameLimit2();
        DataSystemResourceDetailDTO hiveDatabaseDetailDTO = createHiveDatabaseResourceDetailDTO();
        final Map<String, String> hiveTableMap = hiveTables.stream().collect(Collectors.toMap(it -> it, it -> it));
        
        when(mockHiveHelperService.showTables(any())).thenReturn(hiveTables);
        
        hiveDataSystemMetadataServiceImpl.refreshTablesOfDatabase(hiveDatabaseDetailDTO);
        
        // verify
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> dataSystemResourceDetailDTOListCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockDataSystemResourceService, Mockito.times(1))
                .mergeAllChildrenByName(dataSystemResourceDetailDTOListCaptor.capture(), eq(DataSystemResourceType.HIVE_TABLE), eq(hiveDatabaseDetailDTO.getId()));
        
        dataSystemResourceDetailDTOListCaptor.getValue().forEach(it -> {
            Assertions.assertThat(hiveTableMap.get(it.getName())).isEqualTo(it.getName());
            Assertions.assertThat(it.getResourceType()).isEqualTo(DataSystemResourceType.HIVE_TABLE);
            Assertions.assertThat(it.getParentResource().getId()).isEqualTo(hiveDatabaseDetailDTO.getId());
        });
    }
    
    @Test
    public void testRefreshDynamicDataSystemResourceShouldUseParentsProjectRelation() {
        List<ProjectDTO> projects = createProjects();
        DataSystemResourceDetailDTO hiveDetail = createHiveDetail();
        hiveDetail.setProjects(projects);
        when(mockDataSystemResourceService.getDetailById(eq(hiveDetail.getId()))).thenReturn(hiveDetail);
        
        DataSystemResourceDetailDTO hiveDatabaseDetailDTO = createHiveDatabaseResourceDetailDTO();
        hiveDatabaseDetailDTO.setProjects(projects);
        when(mockDataSystemResourceService.getDetailChildren(eq(hiveDetail.getId()), eq(DataSystemResourceType.HIVE_DATABASE))).thenReturn(Lists.newArrayList(hiveDatabaseDetailDTO));
        
        List<String> hiveDatabases = createHiveDatabaseNameLimit2();
        when(mockHiveHelperService.showDatabases()).thenReturn(hiveDatabases);
        
        List<String> hiveTables = createHiveTableNameLimit2();
        when(mockHiveHelperService.showTables(any())).thenReturn(hiveTables);
        
        hiveDataSystemMetadataServiceImpl.refreshDynamicDataSystemResource(hiveDetail.getId());
        
        // verify
        ArgumentCaptor<List<DataSystemResourceDetailDTO>> dataSystemResourceDetailDTOListCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockDataSystemResourceService, Mockito.times(1))
                .mergeAllChildrenByName(dataSystemResourceDetailDTOListCaptor.capture(), eq(DataSystemResourceType.HIVE_DATABASE), eq(hiveDetail.getId()));
        
        dataSystemResourceDetailDTOListCaptor.getValue().forEach(it -> {
            Assertions.assertThat(it.getProjects()).isEqualTo(projects);
        });
        
        Mockito.verify(mockDataSystemResourceService, Mockito.times(1))
                .mergeAllChildrenByName(dataSystemResourceDetailDTOListCaptor.capture(), eq(DataSystemResourceType.HIVE_TABLE), eq(hiveDatabaseDetailDTO.getId()));
        
        dataSystemResourceDetailDTOListCaptor.getValue().forEach(it -> {
            Assertions.assertThat(it.getProjects()).isEqualTo(projects);
        });
    }
    
    @Test
    public void testGetDataCollectionDefinition() {
        DataSystemResourceDTO hiveTable = createHiveTableResourceDTO();
        DataSystemResourceDTO hiveDatabase = createHiveDatabaseResourceDTO();
        
        List<RelationalDatabaseTableField> hiveTableFields = createHiveTableFieldLimit2();
        
        Map<String, RelationalDatabaseTableField> hiveTableFieldMap = hiveTableFields.stream().collect(Collectors.toMap(it -> it.getName(), it -> it));
        
        when(mockDataSystemResourceService.getById(any())).thenReturn(hiveTable);
        when(mockDataSystemResourceService.getParent(any(), any())).thenReturn(hiveDatabase);
        when(mockHiveHelperService.descTable(any(), any())).thenReturn(hiveTableFields);
        DataCollectionDefinition dataCollectionDefinition = hiveDataSystemMetadataServiceImpl.getDataCollectionDefinition(1L);
        Assertions.assertThat(dataCollectionDefinition.getName()).isEqualTo(hiveTable.getName());
        
        dataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions().forEach((k, v) -> {
            Assertions.assertThat(hiveTableFieldMap.get(k).getName()).isEqualTo(v.getName());
            Assertions.assertThat(hiveTableFieldMap.get(k).getType()).isEqualTo(v.getType());
        });
    }
    
    @Test
    public void testGetDataCollectionDefinitionShouldReturnEmptyDefinitionWhenNoFieldExists() {
        when(mockDataSystemResourceService.getById(any())).thenReturn(createHiveTableResourceDTO());
        when(mockDataSystemResourceService.getParent(any(), any())).thenReturn(createHiveDatabaseResourceDTO());
        when(mockHiveHelperService.descTable(any(), any())).thenReturn(Collections.EMPTY_LIST);
        
        DataCollectionDefinition dataCollectionDefinition = hiveDataSystemMetadataServiceImpl.getDataCollectionDefinition(1L);
        Assertions.assertThat(dataCollectionDefinition.getLowerCaseNameToDataFieldDefinitions()).isEmpty();
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
    
    private DataSystemResourceDTO createHiveTableResourceDTO() {
        return new DataSystemResourceDTO().setName("tb");
    }
    
    private DataSystemResourceDTO createHiveDatabaseResourceDTO() {
        return new DataSystemResourceDTO().setId(1L).setName("db");
    }
    
    private DataSystemResourceDetailDTO createHiveDatabaseResourceDetailDTO() {
        return new DataSystemResourceDetailDTO().setId(1L).setName("db");
    }
    
    private List<RelationalDatabaseTableField> createHiveTableFieldLimit2() {
        return Lists.newArrayList(
                new RelationalDatabaseTableField().setName("f1").setType("int"),
                new RelationalDatabaseTableField().setName("f2").setType("string")
        );
    }
    
    private List<String> createHiveDatabaseNameLimit2() {
        return Lists.newArrayList(
                "db1",
                "db2"
        );
    }
    
    private List<String> createHiveTableNameLimit2() {
        return Lists.newArrayList(
                "tb1",
                "tb2"
        );
    }
    
    private List<ProjectDTO> createProjects() {
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
}
