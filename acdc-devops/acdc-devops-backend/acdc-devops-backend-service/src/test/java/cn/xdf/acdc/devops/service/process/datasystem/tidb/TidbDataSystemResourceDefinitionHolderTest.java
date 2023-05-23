package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemResourceConfigurationDefinition.Cluster;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class TidbDataSystemResourceDefinitionHolderTest {
    
    @Test
    void testGetShouldAsExpect() {
        DataSystemResourceDefinition clusterDefinition = TidbDataSystemResourceDefinitionHolder.get();
        
        // cluster
        Assertions.assertThat(clusterDefinition.getType()).isEqualTo(DataSystemResourceType.TIDB_CLUSTER);
        // cluster configuration
        Assertions.assertThat(clusterDefinition.getConfigurationDefinitions()).containsValues(Cluster.USERNAME, Cluster.PASSWORD, Cluster.SOURCE);
        
        // instance
        DataSystemResourceDefinition serverDefinition = clusterDefinition.getChildren().get(DataSystemResourceType.TIDB_SERVER);
        Assertions.assertThat(serverDefinition).isNotNull();
        Assertions.assertThat(serverDefinition.getConfigurationDefinitions()).containsValues(Instance.HOST, Instance.PORT);
        
        // database
        DataSystemResourceDefinition databaseDefinition = clusterDefinition.getChildren().get(DataSystemResourceType.TIDB_DATABASE);
        Assertions.assertThat(databaseDefinition).isNotNull();
        
        // table
        DataSystemResourceDefinition tableDefinition = databaseDefinition.getChildren().get(DataSystemResourceType.TIDB_TABLE);
        Assertions.assertThat(tableDefinition).isNotNull();
    }
}
