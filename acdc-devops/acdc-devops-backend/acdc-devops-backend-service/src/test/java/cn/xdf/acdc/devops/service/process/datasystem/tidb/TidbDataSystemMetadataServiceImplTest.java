package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceConfigurationDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourceDetailDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemResourceService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
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
public class TidbDataSystemMetadataServiceImplTest {

    @Autowired
    @Qualifier("tidbDataSystemMetadataServiceImpl")
    private DataSystemMetadataService dataSystemMetadataService;

    @MockBean
    private DataSystemResourceService dataSystemResourceService;

    @MockBean
    private MysqlHelperService mysqlHelperService;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testCheckDataSystemShouldPassWhenClusterIdIsNull() {
        // input data system resource detail dto id = null
        DataSystemResourceDetailDTO cluster = new DataSystemResourceDetailDTO().setResourceType(DataSystemResourceType.TIDB_CLUSTER);
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
        Mockito.when(dataSystemResourceService.getDetailChildren(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(DataSystemResourceType.TIDB_SERVER))).thenReturn(instanceDetails);

        dataSystemMetadataService.checkDataSystem(clusterDetail.getId());
    }

    @Test(expected = ServerErrorException.class)
    public void testCheckDataSystemShouldErrorWhenPermissionIsInsufficient() {
        DataSystemResourceDetailDTO clusterDetail = generateCluster();
        List<DataSystemResourceDetailDTO> instanceDetails = generateInstances();

        Mockito.when(dataSystemResourceService.getDetailById(ArgumentMatchers.anyLong())).thenReturn(clusterDetail);
        Mockito.when(dataSystemResourceService.getDetailChildren(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(DataSystemResourceType.TIDB_SERVER))).thenReturn(instanceDetails);

        Mockito.doThrow(ServerErrorException.class).when(mysqlHelperService)
                .checkPermissions(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyList());

        dataSystemMetadataService.checkDataSystem(clusterDetail.getId());
    }

    @Test
    public void testGetDataSystemTypeShouldReturnTidb() {
        Assertions.assertThat(dataSystemMetadataService.getDataSystemType()).isEqualTo(DataSystemType.TIDB);
    }

    @Test
    public void testCheckDatabaseIsCreatedByUserShouldReturnTrueWhenDatabaseNameIsTidb() {
        boolean result = ((TidbDataSystemMetadataServiceImpl) dataSystemMetadataService).checkDatabaseIsCreatedByUser("mysql");
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void testCheckDatabaseIsCreatedByUserShouldReturnFalseWhenDatabaseNameIsACDC() {
        boolean result = ((TidbDataSystemMetadataServiceImpl) dataSystemMetadataService).checkDatabaseIsCreatedByUser("acdc");
        Assertions.assertThat(result).isTrue();
    }

    private DataSystemResourceDetailDTO generateCluster() {
        Map<String, DataSystemResourceConfigurationDTO> clusterConfigurations = new HashMap<>();
        clusterConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Cluster.USERNAME.getName())
                .value("username")
                .build());
        clusterConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Cluster.PASSWORD.getName())
                .value("rnUNrqDMs5a8s8xY8UvILg==")
                .build());

        return new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("cluster")
                .setResourceType(DataSystemResourceType.TIDB_CLUSTER)
                .setDataSystemResourceConfigurations(clusterConfigurations);
    }

    private List<DataSystemResourceDetailDTO> generateInstances() {
        Map<String, DataSystemResourceConfigurationDTO> dataSourceInstanceConfigurations = new HashMap<>();
        dataSourceInstanceConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Instance.HOST.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Instance.HOST.getName())
                .value("6.6.6.2")
                .build());
        dataSourceInstanceConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Instance.PORT.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Instance.PORT.getName())
                .value("6662")
                .build());

        Map<String, DataSystemResourceConfigurationDTO> masterInstanceConfigurations = new HashMap<>();
        masterInstanceConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Instance.HOST.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Instance.HOST.getName())
                .value("6.6.6.2")
                .build());
        masterInstanceConfigurations.put(TidbDataSystemResourceConfigurationDefinition.Instance.PORT.getName(), DataSystemResourceConfigurationDTO.builder()
                .name(TidbDataSystemResourceConfigurationDefinition.Instance.PORT.getName())
                .value("6662")
                .build());

        List<DataSystemResourceDetailDTO> instanceDetails = new ArrayList<>();
        instanceDetails.add(new DataSystemResourceDetailDTO()
                .setId(1L)
                .setName("data_source")
                .setResourceType(DataSystemResourceType.TIDB_SERVER)
                .setDataSystemResourceConfigurations(dataSourceInstanceConfigurations));
        instanceDetails.add(new DataSystemResourceDetailDTO()
                .setId(2L)
                .setName("master")
                .setResourceType(DataSystemResourceType.TIDB_SERVER)
                .setDataSystemResourceConfigurations(masterInstanceConfigurations));

        return instanceDetails;
    }
}
