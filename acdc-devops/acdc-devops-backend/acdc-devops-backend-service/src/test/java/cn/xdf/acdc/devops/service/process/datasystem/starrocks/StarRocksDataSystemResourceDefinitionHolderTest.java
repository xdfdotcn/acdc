package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.FrontEnd;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class StarRocksDataSystemResourceDefinitionHolderTest {
    
    @Test
    void testGetShouldAsExpect() {
        DataSystemResourceDefinition clusterDefinition = StarRocksDataSystemResourceDefinitionHolder.get();
        
        // cluster
        Assertions.assertThat(clusterDefinition.getType()).isEqualTo(DataSystemResourceType.STARROCKS_CLUSTER);
        // cluster configuration
        Assertions.assertThat(clusterDefinition.getConfigurationDefinitions()).containsValues(Cluster.USERNAME, Cluster.PASSWORD, Cluster.SOURCE);
        
        // instance
        DataSystemResourceDefinition serverDefinition = clusterDefinition.getChildren().get(DataSystemResourceType.STARROCKS_FRONTEND);
        Assertions.assertThat(serverDefinition).isNotNull();
        Assertions.assertThat(serverDefinition.getConfigurationDefinitions()).containsValues(FrontEnd.HOST, FrontEnd.JDBC_PORT);
        
        // database
        DataSystemResourceDefinition databaseDefinition = clusterDefinition.getChildren().get(DataSystemResourceType.STARROCKS_DATABASE);
        Assertions.assertThat(databaseDefinition).isNotNull();
        
        // table
        DataSystemResourceDefinition tableDefinition = databaseDefinition.getChildren().get(DataSystemResourceType.STARROCKS_TABLE);
        Assertions.assertThat(tableDefinition).isNotNull();
    }
}
