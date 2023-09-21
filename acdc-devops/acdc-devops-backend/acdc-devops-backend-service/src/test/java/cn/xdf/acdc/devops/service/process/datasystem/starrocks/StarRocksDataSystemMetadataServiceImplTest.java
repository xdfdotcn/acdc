package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.FrontEnd;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.StarRocksHelperService;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class StarRocksDataSystemMetadataServiceImplTest {
    
    @Autowired
    @Qualifier("starRocksDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;
    
    @MockBean
    private DataSystemResourceService dataSystemResourceService;
    
    @MockBean
    private StarRocksHelperService starRocksHelperService;
    
    @Before
    public void setUp() throws Exception {
    
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenClusterIdIsNull() {
        // input data system resource detail dto id = null
        DataSystemResourceDetailDTO cluster = new DataSystemResourceDetailDTO().setResourceType(DataSystemResourceType.STARROCKS_CLUSTER);
        dataSystemMetadataService.checkDataSystem(cluster);
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenClusterInstancesIsEmpty() {
        DataSystemResourceDetailDTO cluster = generateCluster();
        dataSystemMetadataService.checkDataSystem(cluster);
    }
    
    @Test
    public void testCheckDataSystemShouldPassWhenPermissionsIsCorrect() {
        DataSystemResourceDetailDTO clusterDetail = generateCluster();
        List<DataSystemResourceDetailDTO> instanceDetails = generateInstances();
        
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.anyLong())).thenReturn(clusterDetail);
        Mockito.when(dataSystemResourceService.getDetailChildren(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_FRONTEND))).thenReturn(instanceDetails);
        
        dataSystemMetadataService.checkDataSystem(clusterDetail.getId());
    }
    
    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenPermissionIsInsufficient() {
        DataSystemResourceDetailDTO clusterDetail = generateCluster();
        List<DataSystemResourceDetailDTO> instanceDetails = generateInstances();
        
        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.anyLong())).thenReturn(clusterDetail);
        Mockito.when(dataSystemResourceService.getDetailChildren(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(DataSystemResourceType.STARROCKS_FRONTEND))).thenReturn(instanceDetails);
        
        Mockito.doThrow(ServerErrorException.class).when(starRocksHelperService)
                .checkHealth(ArgumentMatchers.any(), ArgumentMatchers.any());
        
        dataSystemMetadataService.checkDataSystem(clusterDetail.getId());
    }
    
    @Test
    public void testGetDataSystemTypeShouldReturnStarRocks() {
        Assertions.assertThat(dataSystemMetadataService.getDataSystemType()).isEqualTo(DataSystemType.STARROCKS);
    }
    
    @Test
    public void testCheckDatabaseIsCreatedByUserShouldReturnTrueWhenDatabaseNameIsStarRocks() {
        boolean result = ((StarRocksDataSystemMetadataServiceImpl) dataSystemMetadataService).checkDatabaseIsCreatedByUser("information_schema");
        Assertions.assertThat(result).isFalse();
    }
    
    @Test
    public void testCheckDatabaseIsCreatedByUserShouldReturnFalseWhenDatabaseNameIsACDC() {
        boolean result = ((StarRocksDataSystemMetadataServiceImpl) dataSystemMetadataService).checkDatabaseIsCreatedByUser("acdc");
        Assertions.assertThat(result).isTrue();
    }
    
    private DataSystemResourceDetailDTO generateCluster() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap<>();
        clusterConfigurations.put(StarRocksDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName(), new DataSystemResourceConfigurationDTO()
                .setName(StarRocksDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName())
                .setValue("username"));
        clusterConfigurations.put(StarRocksDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName(), new DataSystemResourceConfigurationDTO()
                .setName(StarRocksDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName())
                .setValue("rnUNrqDMs5a8s8xY8UvILg=="));
        
        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("cluster")
                .setResourceType(DataSystemResourceType.STARROCKS_CLUSTER)
                .setDataSystemResourceConfigurations(clusterConfigurations);
    }
    
    private List<DataSystemResourceDetailDTO> generateInstances() {
        Map<String, DataSystemResourceConfigurationDTO> dataSourceInstanceConfigurations = new HashMap<>();
        dataSourceInstanceConfigurations.put(FrontEnd.HOST.getName(), new DataSystemResourceConfigurationDTO()
                .setName(FrontEnd.HOST.getName())
                .setValue("6.6.6.2"));
        dataSourceInstanceConfigurations.put(FrontEnd.JDBC_PORT.getName(), new DataSystemResourceConfigurationDTO()
                .setName(FrontEnd.JDBC_PORT.getName())
                .setValue("6662"));
        
        Map<String, DataSystemResourceConfigurationDTO> masterInstanceConfigurations = new HashMap<>();
        masterInstanceConfigurations.put(FrontEnd.HOST.getName(), new DataSystemResourceConfigurationDTO()
                .setName(FrontEnd.HOST.getName())
                .setValue("6.6.6.2"));
        masterInstanceConfigurations.put(FrontEnd.JDBC_PORT.getName(), new DataSystemResourceConfigurationDTO()
                .setName(FrontEnd.JDBC_PORT.getName())
                .setValue("6662"));
        
        List<DataSystemResourceDetailDTO> instanceDetails = new ArrayList<>();
        instanceDetails.add(new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("fe_1")
                .setResourceType(DataSystemResourceType.STARROCKS_FRONTEND)
                .setDataSystemResourceConfigurations(dataSourceInstanceConfigurations));
        instanceDetails.add(new DataSystemResourceDetailDTO()
                .setId(2L)
                .setName("fe_2")
                .setResourceType(DataSystemResourceType.STARROCKS_FRONTEND)
                .setDataSystemResourceConfigurations(masterInstanceConfigurations));
        
        return instanceDetails;
    }
}
