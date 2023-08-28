package cn.xdf.acdc.devops.service.process.datasystem.impl;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.DataSystemResourceDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.enumeration.QueryScope;
import cn.xdf.acdc.devops.core.domain.query.DataSystemResourceQuery;
import cn.xdf.acdc.devops.repository.DataSystemResourceConfigurationRepository;
import cn.xdf.acdc.devops.repository.DataSystemResourceRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.repository.UserRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class DataSystemResourceServiceImplTest {
    
    @Autowired
    private DataSystemResourceRepository dataSystemResourceRepository;
    
    @Autowired
    private DataSystemResourceService dataSystemResourceService;
    
    @Autowired
    private DataSystemResourceConfigurationRepository dataSystemResourceConfigurationRepository;
    
    @Autowired
    private EntityManager entityManager;
    
    @Autowired
    private ProjectRepository projectRepository;
    
    @Autowired
    private UserRepository userRepository;
    
    @MockBean
    private DataSystemServiceManager dataSystemServiceManager;
    
    @Mock
    private DataSystemMetadataService dataSystemMetadataService;
    
    @Before
    public void setUp() {
        when(dataSystemServiceManager.getDataSystemMetadataService(any())).thenReturn(dataSystemMetadataService);
    }
    
    @Test
    public void testGetByIdShouldPass() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getById(saved.get(0).getId());
    }
    
    private List<DataSystemResourceDO> saveDataSystemResources() {
        List<DataSystemResourceDO> result = new ArrayList<>();
        
        // cluster
        Set<DataSystemResourceConfigurationDO> mysqlClusterConfigurations = new HashSet<>();
        mysqlClusterConfigurations.add(new DataSystemResourceConfigurationDO().setName("configuration_1").setValue("value_1"));
        mysqlClusterConfigurations.add(new DataSystemResourceConfigurationDO().setName("configuration_2").setValue("value_2"));
        
        DataSystemResourceDO mysqlCluster = new DataSystemResourceDO()
                .setName("mysql_cluster")
                .setDescription("mysql_cluster")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemResourceConfigurations(mysqlClusterConfigurations);
        result.add(mysqlCluster);
        
        mysqlClusterConfigurations.forEach(each -> each.setDataSystemResource(mysqlCluster));
        
        // instance 1
        Set<DataSystemResourceConfigurationDO> mysqlInstance1Configurations = new HashSet<>();
        mysqlInstance1Configurations.add(new DataSystemResourceConfigurationDO().setName("configuration_3").setValue("value_3"));
        mysqlInstance1Configurations.add(new DataSystemResourceConfigurationDO().setName("configuration_4").setValue("value_4"));
        
        DataSystemResourceDO mysqlInstance1 = new DataSystemResourceDO()
                .setName("mysql_instance_1")
                .setDescription("mysql_instance_1")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_INSTANCE)
                .setParentResource(mysqlCluster)
                .setDataSystemResourceConfigurations(mysqlInstance1Configurations);
        result.add(mysqlInstance1);
        
        mysqlInstance1Configurations.forEach(each -> each.setDataSystemResource(mysqlInstance1));
        
        // instance 2
        Set<DataSystemResourceConfigurationDO> mysqlInstance2Configurations = new HashSet<>();
        mysqlInstance2Configurations.add(new DataSystemResourceConfigurationDO().setName("configuration_5").setValue("value_5"));
        mysqlInstance2Configurations.add(new DataSystemResourceConfigurationDO().setName("configuration_6").setValue("value_6"));
        
        DataSystemResourceDO mysqlInstance2 = new DataSystemResourceDO()
                .setName("mysql_instance_2")
                .setDescription("mysql_instance_2")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_INSTANCE)
                .setParentResource(mysqlCluster)
                .setDataSystemResourceConfigurations(mysqlInstance2Configurations);
        result.add(mysqlInstance2);
        
        mysqlInstance2Configurations.forEach(each -> each.setDataSystemResource(mysqlInstance2));
        
        // database 1originalPassword
        DataSystemResourceDO mysqlDatabase1 = new DataSystemResourceDO()
                .setName("mysql_database_1")
                .setDescription("mysql_database_1")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
                .setParentResource(mysqlCluster);
        result.add(mysqlDatabase1);
        
        DataSystemResourceDO mysqlTable1 = new DataSystemResourceDO()
                .setName("mysql_table_1")
                .setDescription("mysql_table_1")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_TABLE)
                .setParentResource(mysqlDatabase1);
        result.add(mysqlTable1);
        
        DataSystemResourceDO mysqlTable2 = new DataSystemResourceDO()
                .setName("mysql_table_2")
                .setDescription("mysql_table_2")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_TABLE)
                .setParentResource(mysqlDatabase1);
        result.add(mysqlTable2);
        
        // database 2
        DataSystemResourceDO mysqlDatabase2 = new DataSystemResourceDO()
                .setName("mysql_database_2")
                .setDescription("mysql_database_2")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_DATABASE)
                .setParentResource(mysqlCluster);
        result.add(mysqlDatabase2);
        
        DataSystemResourceDO mysqlTable3 = new DataSystemResourceDO()
                .setName("mysql_table_3")
                .setDescription("mysql_table_3")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_TABLE)
                .setParentResource(mysqlDatabase2);
        result.add(mysqlTable3);
        
        return dataSystemResourceRepository.saveAll(result);
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetByIdShouldErrorWhenInputNotExistsId() {
        dataSystemResourceService.getById(1L);
    }
    
    @Test
    public void testGetByIdsShouldPass() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getByIds(saved.stream().map(DataSystemResourceDO::getId).collect(Collectors.toList()));
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetByIdsShouldErrorWhenInputNotExistId() {
        dataSystemResourceService.getByIds(Arrays.asList(1L));
    }
    
    @Test
    public void testGetDetailByIdShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        for (DataSystemResourceDO each : saved) {
            DataSystemResourceDetailDTO detail = dataSystemResourceService.getDetailById(each.getId());
            each.getDataSystemResourceConfigurations()
                    .forEach(eachConfiguration -> Assertions.assertThat(detail.getDataSystemResourceConfigurations().get(eachConfiguration.getName()).getValue())
                            .isEqualTo(eachConfiguration.getValue()));
        }
    }
    
    @Test
    public void testGetChildrenShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDTO> children = dataSystemResourceService.getChildren(saved.get(0).getId(), DataSystemResourceType.MYSQL_DATABASE);
        
        Assertions.assertThat(children.size()).isEqualTo(2);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetChildrenShouldErrorWhenResourceIdIsNull() {
        dataSystemResourceService.getChildren(null, DataSystemResourceType.MYSQL_DATABASE);
    }
    
    @Test
    public void testGetChildrenShouldReturnEmptyListWhenResourceIdNotExists() {
        List<DataSystemResourceDTO> children = dataSystemResourceService.getChildren(1L, DataSystemResourceType.MYSQL_DATABASE);
        
        Assertions.assertThat(children.size()).isEqualTo(0);
    }
    
    @Test
    public void testGetChildrenShouldReturnEmptyListWhenResourceHasNoChildrenWithTargetType() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDTO> children = dataSystemResourceService.getChildren(saved.get(0).getId(), DataSystemResourceType.MYSQL_TABLE);
        
        Assertions.assertThat(children.size()).isEqualTo(0);
    }
    
    @Test
    public void testGetDetailChildrenShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDetailDTO> children = dataSystemResourceService.getDetailChildren(saved.get(0).getId(), DataSystemResourceType.MYSQL_INSTANCE);
        
        Assertions.assertThat(children.size()).isEqualTo(2);
        for (DataSystemResourceDO each : saved) {
            for (DataSystemResourceDetailDTO eachChildren : children) {
                if (each.getId().equals(eachChildren.getId())) {
                    for (DataSystemResourceConfigurationDO eachConfiguration : each.getDataSystemResourceConfigurations()) {
                        Assertions.assertThat(eachChildren.getDataSystemResourceConfigurations().get(eachConfiguration.getName()).getValue()).isEqualTo(eachConfiguration.getValue());
                    }
                }
            }
        }
    }
    
    @Test
    public void testGetDetailChildrenShouldAsExpectWhenConfigurationValueMatched() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDO instance1 = saved.get(1);
        DataSystemResourceConfigurationDO dataSystemResourceConfiguration = instance1.getDataSystemResourceConfigurations().stream().findFirst().get();
        List<DataSystemResourceDetailDTO> children = dataSystemResourceService
                .getDetailChildren(saved.get(0).getId(), DataSystemResourceType.MYSQL_INSTANCE, dataSystemResourceConfiguration.getName(), dataSystemResourceConfiguration.getValue());
        
        Assertions.assertThat(children.size()).isEqualTo(1);
        Assertions.assertThat(children.get(0).getId()).isEqualTo(instance1.getId());
    }
    
    @Test
    public void testGetDetailChildrenShouldReturnEmptyListWhenConfigurationValueNotMatch() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDetailDTO> children = dataSystemResourceService
                .getDetailChildren(saved.get(0).getId(), DataSystemResourceType.MYSQL_INSTANCE, "not_exist_name", "not_exist_value");
        
        Assertions.assertThat(children.size()).isEqualTo(0);
    }
    
    @Test
    public void testGetDataSystemTypeShouldPass() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getDataSystemType(saved.get(0).getId());
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetDataSystemTypeShouldErrorWhenIdNotExists() {
        dataSystemResourceService.getDataSystemType(1L);
    }
    
    @Test
    public void testGetParentShouldPassWhenExistsParent() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getParent(saved.get(1).getId(), DataSystemResourceType.MYSQL_CLUSTER);
    }
    
    @Test
    public void testGetParentShouldPassWhenExistsGrandParent() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getParent(saved.get(saved.size() - 1).getId(), DataSystemResourceType.MYSQL_CLUSTER);
    }
    
    @Test(expected = EntityNotFoundException.class)
    public void testGetParentShouldErrorWhenNotExistsParentWithTargetType() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getParent(saved.get(saved.size() - 1).getId(), DataSystemResourceType.HIVE);
    }
    
    @Test
    public void testGetDetailParentShouldPassWhenExistsParent() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        dataSystemResourceService.getDetailParent(saved.get(1).getId(), DataSystemResourceType.MYSQL_CLUSTER);
    }
    
    @Test
    public void testQueryShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        // query all instances
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_INSTANCE));
        
        List<DataSystemResourceDTO> queriedInstances = dataSystemResourceService.query(query);
        
        Assertions.assertThat(queriedInstances.size()).isEqualTo(2);
        queriedInstances.forEach(each -> Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_INSTANCE));
    }
    
    @Test
    public void testQueryDetailShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        // query all instances of cluster
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_INSTANCE));
        
        List<DataSystemResourceDetailDTO> queriedInstances = dataSystemResourceService.queryDetail(query);
        
        Assertions.assertThat(queriedInstances.size()).isEqualTo(2);
        queriedInstances.forEach(each -> Assertions.assertThat(each.getResourceType()).isEqualTo(DataSystemResourceType.MYSQL_INSTANCE));
    }
    
    @Test
    public void testPagedQueryShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        // count tables
        int tableCount = 0;
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_TABLE)) {
                tableCount++;
            }
        }
        
        // query all table
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_TABLE));
        query.setCurrent(1);
        query.setPageSize(1);
        
        Page<DataSystemResourceDTO> pagedQueriedTables = dataSystemResourceService.pagedQuery(query);
        
        Assertions.assertThat(pagedQueriedTables.stream().count()).isEqualTo(1);
        Assertions.assertThat(pagedQueriedTables.getTotalElements()).isEqualTo(tableCount);
        Assertions.assertThat(pagedQueriedTables.getTotalPages()).isEqualTo(tableCount);
    }
    
    @Test
    public void testPagedQueryWithProjectIdShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        Long anyProjectId = null;
        for (int i = 0; i < 2; i++) {
            ProjectDO project = saveProject();
            if (Objects.isNull(anyProjectId)) {
                anyProjectId = project.getId();
            }
            saved.get(i).getProjects().add(project);
        }
        
        entityManager.flush();
        entityManager.clear();
        
        DataSystemResourceQuery query = new DataSystemResourceQuery().setProjectIds(Lists.newArrayList(anyProjectId));
        Page<DataSystemResourceDTO> pagedQueriedTables = dataSystemResourceService.pagedQuery(query);
        
        Assertions.assertThat(pagedQueriedTables.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(pagedQueriedTables.getTotalElements()).isEqualTo(1);
        Assertions.assertThat(pagedQueriedTables.getTotalPages()).isEqualTo(1);
        Assertions.assertThat(dataSystemResourceRepository.findAll().size()).isEqualTo(saved.size());
    }
    
    private ProjectDO saveProjectWithUser() {
        UserDO userDO = saveUser("user");
        return projectRepository.save(new ProjectDO().setId(1L).setName("project").setUsers(Sets.newHashSet(userDO)));
    }
    
    private UserDO saveUser(final String domainAccount) {
        UserDO user = new UserDO();
        user.setEmail(domainAccount + "@acdc.io");
        user.setName(domainAccount);
        user.setDomainAccount(domainAccount);
        user.setPassword("user");
        user.setCreatedBy("user");
        return userRepository.save(user);
    }
    
    private ProjectDO saveProject() {
        return projectRepository.save(new ProjectDO().setName("project"));
    }
    
    @Test
    public void testPagedQueryDetailShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
    
        ProjectDO project = saveProjectWithUser();
        for (DataSystemResourceDO saveOne: saved) {
            saveOne.getProjects().add(project);
        }
    
        entityManager.flush();
        entityManager.clear();
        
        // count tables
        int tableCount = 0;
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_TABLE)) {
                tableCount++;
            }
        }
        
        // query all table
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_TABLE));
        query.setProjectIds(Lists.newArrayList(1L, 2L));
        query.setCurrent(1);
        query.setPageSize(1);
        
        Page<DataSystemResourceDetailDTO> pagedQueriedTables = dataSystemResourceService.pagedQueryDetail(query);
        
        Assertions.assertThat(pagedQueriedTables.stream().count()).isEqualTo(1);
        Assertions.assertThat(pagedQueriedTables.getTotalElements()).isEqualTo(tableCount);
        Assertions.assertThat(pagedQueriedTables.getTotalPages()).isEqualTo(tableCount);
    
        // query all table with unknown project
        DataSystemResourceQuery query2 = new DataSystemResourceQuery();
        query2.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_TABLE));
        query2.setProjectIds(Lists.newArrayList(2L));
        query2.setCurrent(1);
        query2.setPageSize(1);
    
        Page<DataSystemResourceDetailDTO> pagedQueriedTables2 = dataSystemResourceService.pagedQueryDetail(query2);
    
        Assertions.assertThat(pagedQueriedTables2.stream().count()).isEqualTo(0);
    }
    
    @Test
    public void testPagedQueryDetailShouldReturnAccordingToQueryUser() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        ProjectDO project = saveProjectWithUser();
        for (DataSystemResourceDO saveOne: saved) {
            saveOne.getProjects().add(project);
        }
        
        entityManager.flush();
        entityManager.clear();
        
        // count tables
        int tableCount = 0;
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_TABLE)) {
                tableCount++;
            }
        }
        
        // query all table with the user
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_TABLE));
        query.setCurrent(1);
        query.setPageSize(1);
        query.setScope(QueryScope.CURRENT_USER);
        query.setMemberDomainAccount("user");
        Page<DataSystemResourceDetailDTO> pagedQueriedTables = dataSystemResourceService.pagedQueryDetail(query);
        
        Assertions.assertThat(pagedQueriedTables.stream().count()).isEqualTo(1);
        Assertions.assertThat(pagedQueriedTables.getTotalElements()).isEqualTo(tableCount);
        Assertions.assertThat(pagedQueriedTables.getTotalPages()).isEqualTo(tableCount);
    
        // query all table with unknown user
        DataSystemResourceQuery query2 = new DataSystemResourceQuery();
        query2.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_TABLE));
        query2.setCurrent(1);
        query2.setPageSize(1);
        query2.setScope(QueryScope.CURRENT_USER);
        query2.setMemberDomainAccount("user2");
        Page<DataSystemResourceDetailDTO> pagedQueriedTables2 = dataSystemResourceService.pagedQueryDetail(query2);
    
        Assertions.assertThat(pagedQueriedTables2.stream().count()).isEqualTo(0);
    }
    
    @Test
    public void testMergeAllChildrenByNameShouldCreateNewResource() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        List<DataSystemResourceDetailDTO> actualDatabases = new ArrayList<>();
        // add existed databases
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE)) {
                actualDatabases.add(new DataSystemResourceDetailDTO(each));
            }
        }
        // add 3 new databases
        for (int i = 0; i < 3; i++) {
            actualDatabases.add(new DataSystemResourceDetailDTO()
                    .setParentResource(new DataSystemResourceDetailDTO(saved.get(0)))
                    .setName("new_database_" + i)
                    .setDescription("new_database_" + i)
                    .setDataSystemType(DataSystemType.MYSQL)
                    .setResourceType(DataSystemResourceType.MYSQL_DATABASE));
        }
        
        List<DataSystemResourceDetailDTO> mergedDatabases = dataSystemResourceService.mergeAllChildrenByName(actualDatabases, DataSystemResourceType.MYSQL_DATABASE, saved.get(0).getId());
        
        // assert
        // all ids in merged and actual resources are some
        Assertions.assertThat(mergedDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()));
        
        // query all databases
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_DATABASE));
        List<DataSystemResourceDTO> queriedDatabases = dataSystemResourceService.query(query);
        // all ids in queried and actual resources are some
        Assertions.assertThat(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(queriedDatabases.stream().map(DataSystemResourceDTO::getName).collect(Collectors.toList()));
    }
    
    @Test
    public void testMergeAllChildrenByNameShouldUpdateResource() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        List<DataSystemResourceDetailDTO> actualDatabases = new ArrayList<>();
        // update description and configuration
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE)) {
                DataSystemResourceDetailDTO updated = new DataSystemResourceDetailDTO(each);
                updated.setDescription("updated_description");
                
                DataSystemResourceConfigurationDTO updatedConfiguration = new DataSystemResourceConfigurationDTO();
                updatedConfiguration.setName("updated_configuration_name");
                updatedConfiguration.setValue("updated_configuration_value");
                
                updated.getDataSystemResourceConfigurations().put(updatedConfiguration.getName(), updatedConfiguration);
                actualDatabases.add(updated);
            }
        }
        
        List<DataSystemResourceDetailDTO> mergedDatabases = dataSystemResourceService.mergeAllChildrenByName(actualDatabases, DataSystemResourceType.MYSQL_DATABASE, saved.get(0).getId());
        
        // assert
        // check if merged result is same to actual
        Assertions.assertThat(mergedDatabases.size()).isEqualTo(actualDatabases.size());
        mergedDatabases.forEach(each -> {
            Assertions.assertThat(each.getDescription()).isEqualTo("updated_description");
            Assertions.assertThat(each.getDataSystemResourceConfigurations().size()).isEqualTo(1);
            Assertions.assertThat(each.getDataSystemResourceConfigurations().get("updated_configuration_name").getValue()).isEqualTo("updated_configuration_value");
        });
        
        // check if description and configuration has been saved
        // query all not deleted databases
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_DATABASE));
        query.setDeleted(Boolean.FALSE);
        List<DataSystemResourceDetailDTO> queriedDatabases = dataSystemResourceService.queryDetail(query);
        queriedDatabases.forEach(each -> {
            Assertions.assertThat(each.getDescription()).isEqualTo("updated_description");
            Assertions.assertThat(each.getDataSystemResourceConfigurations().size()).isEqualTo(1);
            Assertions.assertThat(each.getDataSystemResourceConfigurations().get("updated_configuration_name").getValue()).isEqualTo("updated_configuration_value");
        });
    }
    
    @Test
    public void testMergeAllChildrenByNameShouldDeleteResource() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        List<DataSystemResourceDetailDTO> actualDatabases = new ArrayList<>();
        // add existed databases
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE)) {
                actualDatabases.add(new DataSystemResourceDetailDTO(each));
            }
        }
        // delete one database
        final DataSystemResourceDetailDTO deletedDatabases = actualDatabases.remove(0);
        
        List<DataSystemResourceDetailDTO> mergedDatabases = dataSystemResourceService.mergeAllChildrenByName(actualDatabases, DataSystemResourceType.MYSQL_DATABASE, saved.get(0).getId());
        
        // assert
        // all ids in merged and actual resources are some
        Assertions.assertThat(mergedDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()));
        
        // query all not deleted databases
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_DATABASE));
        query.setDeleted(Boolean.FALSE);
        List<DataSystemResourceDTO> queriedDatabases = dataSystemResourceService.query(query);
        // all ids in queried and actual resources are some
        Assertions.assertThat(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(queriedDatabases.stream().map(DataSystemResourceDTO::getName).collect(Collectors.toList()));
        
        // check if deleted database has been logical deleted
        // query all deleted databases
        query.setDeleted(Boolean.TRUE);
        queriedDatabases = dataSystemResourceService.query(query);
        Assertions.assertThat(queriedDatabases.size()).isEqualTo(1);
        Assertions.assertThat(queriedDatabases.get(0).getId()).isEqualTo(deletedDatabases.getId());
    }
    
    @Test(expected = ClientErrorException.class)
    public void testMergeAllChildrenByNameShouldErrorWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        List<DataSystemResourceDetailDTO> actualDatabases = new ArrayList<>();
        // add existed databases
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE)) {
                actualDatabases.add(new DataSystemResourceDetailDTO(each));
            }
        }
        // add 3 new databases
        for (int i = 0; i < 3; i++) {
            actualDatabases.add(new DataSystemResourceDetailDTO()
                    .setParentResource(new DataSystemResourceDetailDTO(saved.get(0)))
                    .setName("new_database_" + i)
                    .setDescription("new_database_" + i)
                    .setDataSystemType(DataSystemType.MYSQL)
                    .setResourceType(DataSystemResourceType.MYSQL_DATABASE));
        }
        
        dataSystemResourceService.mergeAllChildrenByName(actualDatabases, DataSystemResourceType.MYSQL_DATABASE, saved.get(0).getId());
    }
    
    @Test
    public void testMergeAllChildrenByNameWithoutCheckShouldAsExpect() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        List<DataSystemResourceDetailDTO> actualDatabases = new ArrayList<>();
        // add existed databases
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE)) {
                actualDatabases.add(new DataSystemResourceDetailDTO(each));
            }
        }
        // add 3 new databases
        for (int i = 0; i < 3; i++) {
            actualDatabases.add(new DataSystemResourceDetailDTO()
                    .setParentResource(new DataSystemResourceDetailDTO(saved.get(0)))
                    .setName("new_database_" + i)
                    .setDescription("new_database_" + i)
                    .setDataSystemType(DataSystemType.MYSQL)
                    .setResourceType(DataSystemResourceType.MYSQL_DATABASE));
        }
        
        List<DataSystemResourceDetailDTO> mergedDatabases = dataSystemResourceService
                .mergeAllChildrenByNameWithoutCheck(actualDatabases, DataSystemResourceType.MYSQL_DATABASE, saved.get(0).getId());
        
        // assert
        // all ids in merged and actual resources are some
        Assertions.assertThat(mergedDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()));
        
        // query all databases
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_DATABASE));
        List<DataSystemResourceDTO> queriedDatabases = dataSystemResourceService.query(query);
        // all ids in queried and actual resources are some
        Assertions.assertThat(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(queriedDatabases.stream().map(DataSystemResourceDTO::getName).collect(Collectors.toList()));
    }
    
    @Test
    public void testMergeAllChildrenByNameWithoutCheckShouldPassWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        
        List<DataSystemResourceDetailDTO> actualDatabases = new ArrayList<>();
        // add existed databases
        for (DataSystemResourceDO each : saved) {
            if (each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE)) {
                actualDatabases.add(new DataSystemResourceDetailDTO(each));
            }
        }
        // add 3 new databases
        for (int i = 0; i < 3; i++) {
            actualDatabases.add(new DataSystemResourceDetailDTO()
                    .setParentResource(new DataSystemResourceDetailDTO(saved.get(0)))
                    .setName("new_database_" + i)
                    .setDescription("new_database_" + i)
                    .setDataSystemType(DataSystemType.MYSQL)
                    .setResourceType(DataSystemResourceType.MYSQL_DATABASE));
        }
        
        List<DataSystemResourceDetailDTO> mergedDatabases = dataSystemResourceService
                .mergeAllChildrenByNameWithoutCheck(actualDatabases, DataSystemResourceType.MYSQL_DATABASE, saved.get(0).getId());
        
        // assert
        // all ids in merged and actual resources are some
        Assertions.assertThat(mergedDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()));
        
        // query all databases
        DataSystemResourceQuery query = new DataSystemResourceQuery();
        query.setParentResourceId(saved.get(0).getId());
        query.setResourceTypes(Arrays.asList(DataSystemResourceType.MYSQL_DATABASE));
        List<DataSystemResourceDTO> queriedDatabases = dataSystemResourceService.query(query);
        // all ids in queried and actual resources are some
        Assertions.assertThat(actualDatabases.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()))
                .containsAll(queriedDatabases.stream().map(DataSystemResourceDTO::getName).collect(Collectors.toList()));
    }
    
    @Test
    public void testCreateShouldPassWhenCheckPass() {
        DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                .setName("new_resource")
                .setDescription("new_resource")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        DataSystemResourceDetailDTO savedResource = dataSystemResourceService.create(resource);
        // check if resource has bean saved in db
        dataSystemResourceRepository.getOne(savedResource.getId());
    }
    
    @Test
    public void testCreateShouldEncryptSensitiveConfiguration() {
        DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                .setName("new_resource")
                .setDescription("new_resource")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        
        String originalPassword = "6662";
        resource.getDataSystemResourceConfigurations().put(
                Authorization.PASSWORD.getName(),
                new DataSystemResourceConfigurationDTO()
                        .setName(Authorization.PASSWORD.getName())
                        .setValue(originalPassword)
        );
        DataSystemResourceDetailDTO savedResource = dataSystemResourceService.create(resource);
        // check if resource has bean saved in db
        DataSystemResourceDO dataSystemResource = dataSystemResourceRepository.getOne(savedResource.getId());
        Assertions.assertThat(dataSystemResource.getDataSystemResourceConfigurations().stream().findAny().get().getValue())
                .isEqualTo(EncryptUtil.encrypt(originalPassword));
    }
    
    @Test(expected = ClientErrorException.class)
    public void testCreateShouldErrorWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        DataSystemResourceDetailDTO resource = new DataSystemResourceDetailDTO()
                .setName("new_resource")
                .setDescription("new_resource")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER)
                .setDataSystemResourceConfigurations(new HashMap<>());
        dataSystemResourceService.create(resource);
    }
    
    @Test
    public void testBatchCreateShouldPassWhenCheckPass() {
        List<DataSystemResourceDetailDTO> toCreateResources = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            toCreateResources.add(new DataSystemResourceDetailDTO()
                    .setName("new_resource_" + i)
                    .setDescription("new_resource_" + i)
                    .setDataSystemType(DataSystemType.MYSQL)
                    .setResourceType(DataSystemResourceType.MYSQL_CLUSTER));
        }
        
        dataSystemResourceService.batchCreate(toCreateResources);
        List<DataSystemResourceDO> queriedClusters = dataSystemResourceRepository.findAll();
        
        Assertions.assertThat(queriedClusters.stream().map(DataSystemResourceDO::getName).collect(Collectors.toList()))
                .containsAll(toCreateResources.stream().map(DataSystemResourceDetailDTO::getName).collect(Collectors.toList()));
    }
    
    @Test(expected = ClientErrorException.class)
    public void testBatchCreateShouldErrorWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        List<DataSystemResourceDetailDTO> toCreateResources = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            toCreateResources.add(new DataSystemResourceDetailDTO()
                    .setName("new_resource_" + i)
                    .setDescription("new_resource_" + i)
                    .setDataSystemType(DataSystemType.MYSQL)
                    .setResourceType(DataSystemResourceType.MYSQL_CLUSTER));
        }
        
        dataSystemResourceService.batchCreate(toCreateResources);
    }
    
    @Test
    public void testUpdateShouldUpdateDescriptionWhenDescriptionChanged() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDetailDTO toUpdateResource = dataSystemResourceService.getDetailById(saved.get(0).getId());
        toUpdateResource.setDescription("updated_description");
        
        dataSystemResourceService.update(toUpdateResource);
        DataSystemResourceDO updatedResource = dataSystemResourceRepository.getOne(toUpdateResource.getId());
        Assertions.assertThat(updatedResource.getDescription()).isEqualTo(toUpdateResource.getDescription());
    }
    
    @Test
    public void testUpdateShouldEncryptSensitiveConfiguration() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDetailDTO toUpdateResource = dataSystemResourceService.getDetailById(saved.get(0).getId());
        String originalPassword = "6662";
        toUpdateResource.getDataSystemResourceConfigurations().put(
                Authorization.PASSWORD.getName(),
                new DataSystemResourceConfigurationDTO()
                        .setName(Authorization.PASSWORD.getName())
                        .setValue(originalPassword)
        );
        
        dataSystemResourceService.update(toUpdateResource);
        DataSystemResourceDO updatedResource = dataSystemResourceRepository.getOne(toUpdateResource.getId());
        updatedResource.getDataSystemResourceConfigurations().forEach(each -> {
            if (each.getName().equals(Authorization.PASSWORD.getName())) {
                Assertions.assertThat(each.getValue()).isEqualTo(EncryptUtil.encrypt(originalPassword));
            }
        });
    }
    
    @Test
    public void testUpdateShouldMergeConfigurationWhenConfigurationValueChanged() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDetailDTO toUpdateResource = dataSystemResourceService.getDetailById(saved.get(0).getId());
        toUpdateResource.getDataSystemResourceConfigurations().values().forEach(each -> {
            each.setId(null);
            each.setValue("updated_configuration_value");
        });
        
        long configurationCountBeforeUpdate = dataSystemResourceConfigurationRepository.count();
        dataSystemResourceService.update(toUpdateResource);
        
        long configurationCountAfterUpdate = dataSystemResourceConfigurationRepository.count();
        Assertions.assertThat(configurationCountAfterUpdate).isEqualTo(configurationCountBeforeUpdate);
        
        DataSystemResourceDO updatedResource = dataSystemResourceRepository.getOne(toUpdateResource.getId());
        Assertions.assertThat(updatedResource.getDataSystemResourceConfigurations().size())
                .isEqualTo(toUpdateResource.getDataSystemResourceConfigurations().size());
        updatedResource.getDataSystemResourceConfigurations().forEach(each -> {
            Assertions.assertThat(each.getValue()).isEqualTo("updated_configuration_value");
        });
    }
    
    @Test
    public void testUpdateShouldCreateNewConfigurationWhenConfigurationAdded() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDetailDTO toUpdateResource = dataSystemResourceService.getDetailById(saved.get(0).getId());
        toUpdateResource.getDataSystemResourceConfigurations().put("new_configuration_name", new DataSystemResourceConfigurationDTO()
                .setName("new_configuration_name")
                .setValue("new_configuration_value"));
        
        dataSystemResourceService.update(toUpdateResource);
        
        DataSystemResourceDetailDTO updatedResource = dataSystemResourceService.getDetailById(toUpdateResource.getId());
        Assertions.assertThat(updatedResource.getDataSystemResourceConfigurations().size())
                .isEqualTo(toUpdateResource.getDataSystemResourceConfigurations().size());
        toUpdateResource.getDataSystemResourceConfigurations().values().forEach(each -> {
            Assertions.assertThat(updatedResource.getDataSystemResourceConfigurations().get(each.getName()).getValue()).isEqualTo(each.getValue());
        });
    }
    
    @Test
    public void testUpdateShouldDeleteConfigurationWhenConfigurationDeleted() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDetailDTO toUpdateResource = dataSystemResourceService.getDetailById(saved.get(0).getId());
        int toDeleteConfigurationCount = toUpdateResource.getDataSystemResourceConfigurations().size();
        toUpdateResource.getDataSystemResourceConfigurations().clear();
        
        long configurationCountBeforeUpdate = dataSystemResourceConfigurationRepository.count();
        dataSystemResourceService.update(toUpdateResource);
        
        long configurationCountAfterUpdate = dataSystemResourceConfigurationRepository.count();
        Assertions.assertThat(configurationCountAfterUpdate).isEqualTo(configurationCountBeforeUpdate - toDeleteConfigurationCount);
        
        DataSystemResourceDO updatedResource = dataSystemResourceRepository.getOne(toUpdateResource.getId());
        Assertions.assertThat(updatedResource.getDataSystemResourceConfigurations()).isEmpty();
    }
    
    @Test(expected = ClientErrorException.class)
    public void testUpdateShouldErrorWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDetailDTO toUpdateResource = dataSystemResourceService.getDetailById(saved.get(0).getId());
        toUpdateResource.setDescription("updated_description");
        
        dataSystemResourceService.update(toUpdateResource);
    }
    
    @Test
    public void testBatchUpdateShouldPassWhenCheckPass() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDetailDTO> toUpdateResources = new ArrayList<>();
        saved.forEach(each -> {
            DataSystemResourceDetailDTO toUpdateResource = new DataSystemResourceDetailDTO(each);
            toUpdateResource.setDescription("updated_description");
            toUpdateResources.add(toUpdateResource);
        });
        dataSystemResourceService.batchUpdate(toUpdateResources);
    }
    
    @Test(expected = ClientErrorException.class)
    public void testBatchUpdateShouldErrorWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDetailDTO> toUpdateResources = new ArrayList<>();
        saved.forEach(each -> {
            DataSystemResourceDetailDTO toUpdateResource = new DataSystemResourceDetailDTO(each);
            toUpdateResource.setDescription("updated_description");
            toUpdateResources.add(toUpdateResource);
        });
        dataSystemResourceService.batchUpdate(toUpdateResources);
    }
    
    @Test
    public void testCreateOrUpdateAllWithoutCheckShouldPass() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDetailDTO> toUpdateOrUpdateResources = new ArrayList<>();
        saved.forEach(each -> {
            DataSystemResourceDetailDTO toUpdateResource = new DataSystemResourceDetailDTO(each);
            toUpdateResource.setDescription("updated_description");
            toUpdateOrUpdateResources.add(toUpdateResource);
        });
        
        DataSystemResourceDetailDTO toCreateResource = new DataSystemResourceDetailDTO()
                .setName("to_create_resource")
                .setDescription("to_create_resource")
                .setDataSystemType(DataSystemType.MYSQL)
                .setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        
        toUpdateOrUpdateResources.add(toCreateResource);
        
        dataSystemResourceService.createOrUpdateAllWithoutCheck(toUpdateOrUpdateResources);
        long resourceCount = dataSystemResourceRepository.count();
        Assertions.assertThat(resourceCount).isEqualTo(toUpdateOrUpdateResources.size());
    }
    
    @Test
    public void testCreateOrUpdateAllWithoutCheckShouldPassWhenCheckNotPass() {
        doThrow(ServerErrorException.class).when(dataSystemMetadataService).checkDataSystem(any(DataSystemResourceDetailDTO.class));
        
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<DataSystemResourceDetailDTO> toUpdateOrUpdateResources = new ArrayList<>();
        saved.forEach(each -> {
            DataSystemResourceDetailDTO toUpdateResource = new DataSystemResourceDetailDTO(each);
            toUpdateResource.setDescription("updated_description");
            toUpdateOrUpdateResources.add(toUpdateResource);
        });
        dataSystemResourceService.createOrUpdateAllWithoutCheck(toUpdateOrUpdateResources);
    }
    
    @Test
    public void testDeleteByIdShouldLogicalDelete() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDO toDeleteResource = saved.get(0);
        
        dataSystemResourceService.deleteById(toDeleteResource.getId());
        
        DataSystemResourceDO deletedResource = dataSystemResourceRepository.getOne(toDeleteResource.getId());
        Assertions.assertThat(deletedResource.getDeleted()).isTrue();
    }
    
    @Test
    public void testDeleteByIdShouldLogicalDeleteAllChildren() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        DataSystemResourceDO toDeleteResource = saved.get(0);
        
        entityManager.flush();
        entityManager.clear();
        
        dataSystemResourceService.deleteById(toDeleteResource.getId());
        
        List<DataSystemResourceDO> allResources = dataSystemResourceRepository.findAll();
        allResources.forEach(each -> Assertions.assertThat(each.getDeleted()).isTrue());
    }
    
    @Test
    public void testBatchDeleteByIdsShouldLogicalDelete() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<Long> toDeleteIds = saved.stream()
                .filter(each -> each.getResourceType().equals(DataSystemResourceType.MYSQL_TABLE))
                .map(DataSystemResourceDO::getId)
                .collect(Collectors.toList());
        
        dataSystemResourceService.batchDeleteByIds(toDeleteIds);
        
        toDeleteIds.forEach(each -> {
            DataSystemResourceDO deletedResource = dataSystemResourceRepository.getOne(each);
            Assertions.assertThat(deletedResource.getDeleted()).isTrue();
        });
    }
    
    @Test
    public void testBatchDeleteByIdsShouldLogicalDeleteAllChildren() {
        List<DataSystemResourceDO> saved = saveDataSystemResources();
        List<Long> toDeleteIds = saved.stream()
                .filter(each -> each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE))
                .map(DataSystemResourceDO::getId)
                .collect(Collectors.toList());
        
        entityManager.flush();
        entityManager.clear();
        
        dataSystemResourceService.batchDeleteByIds(toDeleteIds);
        
        saved.stream().filter(each -> each.getResourceType().equals(DataSystemResourceType.MYSQL_DATABASE) || each.getResourceType().equals(DataSystemResourceType.MYSQL_TABLE))
                .map(DataSystemResourceDO::getId)
                .forEach(each -> {
                    DataSystemResourceDO deletedResource = dataSystemResourceRepository.getOne(each);
                    Assertions.assertThat(deletedResource.getDeleted()).isTrue();
                });
    }
}
