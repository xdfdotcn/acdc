package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class MysqlDataSystemResourceDefinitionHolderTest {
    
    @Test
    void testGetShouldAsExpect() {
        DataSystemResourceDefinition clusterDefinition = MysqlDataSystemResourceDefinitionHolder.get();
        
        // cluster
        Assertions.assertThat(clusterDefinition.getType()).isEqualTo(DataSystemResourceType.MYSQL_CLUSTER);
        // cluster configuration
        Assertions.assertThat(clusterDefinition.getConfigurationDefinitions()).containsValues(Cluster.USERNAME, Cluster.PASSWORD, Cluster.SOURCE);
        
        // instance
        DataSystemResourceDefinition instanceDefinition = clusterDefinition.getChildren().get(DataSystemResourceType.MYSQL_INSTANCE);
        Assertions.assertThat(instanceDefinition).isNotNull();
        Assertions.assertThat(instanceDefinition.getConfigurationDefinitions()).containsValues(Instance.HOST, Instance.PORT, Instance.ROLE_TYPE);
        
        // database
        DataSystemResourceDefinition databaseDefinition = clusterDefinition.getChildren().get(DataSystemResourceType.MYSQL_DATABASE);
        Assertions.assertThat(databaseDefinition).isNotNull();
        
        // table
        DataSystemResourceDefinition tableDefinition = databaseDefinition.getChildren().get(DataSystemResourceType.MYSQL_TABLE);
        Assertions.assertThat(tableDefinition).isNotNull();
    }
}
