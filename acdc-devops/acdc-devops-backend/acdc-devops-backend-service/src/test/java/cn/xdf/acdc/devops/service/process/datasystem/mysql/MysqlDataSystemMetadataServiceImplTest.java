package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemConstant.Metadata.UserPermissionsAndBinlogConfiguration;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MysqlDataSystemMetadataServiceImplTest {
    
    @Autowired
    @Qualifier("mysqlDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private MysqlHelperService mysqlHelperService;
    
    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testGetDataSystemResourceDefinitionShouldAsExpect() {
    
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenPermissionsAndBinlogConfigurationIsCorrect() {
        DataSystemResourceDetailDTO cluster = generateClusterDetail();
        List<DataSystemResourceDetailDTO> instances = generateInstanceDetails();
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instances);
        when(mysqlHelperService.showVariables(any(), any())).thenReturn(generateVariables());
        
        dataSystemMetadataService.checkDataSystem(cluster.getId());
    }
    
    private DataSystemResourceDetailDTO generateClusterDetail() {
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
    
    private List<DataSystemResourceDetailDTO> generateInstanceDetails() {
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
    
    private Map<String, String> generateVariables() {
        Map<String, String> mysqlVariables = new HashMap<>();
        mysqlVariables.put(UserPermissionsAndBinlogConfiguration.SQL_MODE, UserPermissionsAndBinlogConfiguration.EXPECTED_SQL_MODE_VALUE);
        for (int i = 0; i < UserPermissionsAndBinlogConfiguration.TO_CHECK_BINLOG_CONFIGURATION.length; i++) {
            mysqlVariables.put(UserPermissionsAndBinlogConfiguration.TO_CHECK_BINLOG_CONFIGURATION[i],
                    UserPermissionsAndBinlogConfiguration.EXPECTED_BINLOG_CONFIGURATION_VALUE[i]);
        }
        return mysqlVariables;
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenClusterIdIsNull() {
        // input data system resource detail dto id = null
        DataSystemResourceDetailDTO cluster = new DataSystemResourceDetailDTO().setResourceType(DataSystemResourceType.MYSQL_CLUSTER);
        dataSystemMetadataService.checkDataSystem(cluster);
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenClusterHasNoInstance() {
        // input is cluster, and instance is empty
        DataSystemResourceDetailDTO cluster = generateClusterDetail();
        dataSystemMetadataService.checkDataSystem(cluster);
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenInputResourceTypeIsMysqlDatabase() {
        // input a resource typed mysql_database
        DataSystemResourceDetailDTO database = new DataSystemResourceDetailDTO()
                .setId(0L)
                .setResourceType(DataSystemResourceType.MYSQL_DATABASE);
        dataSystemMetadataService.checkDataSystem(database);
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenPermissionIsInsufficient() {
        DataSystemResourceDetailDTO clusterDetail = generateClusterDetail();
        List<DataSystemResourceDetailDTO> instanceDetails = generateInstanceDetails();
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(clusterDetail);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instanceDetails);
        
        doThrow(ServerErrorException.class).when(mysqlHelperService).checkPermissions(any(), any(), anyList());
        
        dataSystemMetadataService.checkDataSystem(clusterDetail.getId());
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenBinlogFormatIsNotRow() {
        DataSystemResourceDetailDTO cluster = generateClusterDetail();
        List<DataSystemResourceDetailDTO> instances = generateInstanceDetails();
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instances);
        
        Map<String, String> mysqlVariables = generateVariables();
        mysqlVariables.put("binlog_format", "some_other_format");
        when(mysqlHelperService.showVariables(any(), any())).thenReturn(mysqlVariables);
        
        dataSystemMetadataService.checkDataSystem(cluster.getId());
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenBinlogExpireLogsDaysIsLessThan4() {
        DataSystemResourceDetailDTO cluster = generateClusterDetail();
        List<DataSystemResourceDetailDTO> instances = generateInstanceDetails();
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instances);
        
        Map<String, String> mysqlVariables = generateVariables();
        mysqlVariables.put("expire_logs_days", "3");
        when(mysqlHelperService.showVariables(any(), any())).thenReturn(mysqlVariables);
        
        dataSystemMetadataService.checkDataSystem(cluster.getId());
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenLogSlaveUpdatesIsOff() {
        DataSystemResourceDetailDTO cluster = generateClusterDetail();
        List<DataSystemResourceDetailDTO> instances = generateInstanceDetails();
        
        when(dataSystemResourceService.getDetailById(anyLong())).thenReturn(cluster);
        when(dataSystemResourceService.getDetailChildren(anyLong(), eq(DataSystemResourceType.MYSQL_INSTANCE))).thenReturn(instances);
        
        Map<String, String> mysqlVariables = generateVariables();
        mysqlVariables.put("log_slave_updates", "OFF");
        when(mysqlHelperService.showVariables(any(), any())).thenReturn(mysqlVariables);
        when(mysqlHelperService.isSlaveInstance(any(), any())).thenReturn(true);
        
        dataSystemMetadataService.checkDataSystem(cluster.getId());
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnMysql() {
        Assertions.assertThat(dataSystemMetadataService.getDataSystemType()).isEqualTo(DataSystemType.MYSQL);
    }
    
    @Test
    public void testCheckDatabaseIsCreatedByUserShouldReturnTureWhenDatabaseNameIsMysql() {
        boolean result = ((MysqlDataSystemMetadataServiceImpl) dataSystemMetadataService).checkDatabaseIsCreatedByUser("mysql");
        Assertions.assertThat(result).isFalse();
    }
    
    @Test
    public void testCheckDatabaseIsCreatedByUserShouldReturnFalseWhenDatabaseNameIsACDC() {
        boolean result = ((MysqlDataSystemMetadataServiceImpl) dataSystemMetadataService).checkDatabaseIsCreatedByUser("acdc");
        Assertions.assertThat(result).isTrue();
    }
    
    @Test
    public void testGetDatabaseDataSystemResourceTypeShouldReturnMysqlDatabase() {
        DataSystemResourceType type = ((MysqlDataSystemMetadataServiceImpl) dataSystemMetadataService).getDatabaseDataSystemResourceType();
        Assertions.assertThat(type).isEqualTo(DataSystemResourceType.MYSQL_DATABASE);
    }
    
    @Test
    public void testGetClusterDataSystemResourceTypeShouldReturnMysqlCluster() {
        DataSystemResourceType type = ((MysqlDataSystemMetadataServiceImpl) dataSystemMetadataService).getClusterDataSystemResourceType();
        Assertions.assertThat(type).isEqualTo(DataSystemResourceType.MYSQL_CLUSTER);
    }
    
    @Test
    public void testGetInstanceDataSystemResourceTypeShouldReturnMysqlInstance() {
        DataSystemResourceType type = ((MysqlDataSystemMetadataServiceImpl) dataSystemMetadataService).getInstanceDataSystemResourceType();
        Assertions.assertThat(type).isEqualTo(DataSystemResourceType.MYSQL_INSTANCE);
    }
}
