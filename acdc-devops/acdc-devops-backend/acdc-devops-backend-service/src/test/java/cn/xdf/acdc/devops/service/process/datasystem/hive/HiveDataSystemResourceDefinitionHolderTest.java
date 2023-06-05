package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hive;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class HiveDataSystemResourceDefinitionHolderTest {
    
    @Test
    void testGetShouldAsExpect() {
        DataSystemResourceDefinition hiveDefinition = HiveDataSystemResourceDefinitionHolder.get();
        
        // cluster
        Assertions.assertThat(hiveDefinition.getType()).isEqualTo(DataSystemResourceType.HIVE);
        // cluster configuration
        Assertions.assertThat(hiveDefinition.getConfigurationDefinitions()).containsValues(Hive.HIVE_METASTORE_URIS,
                Hive.HDFS_NAME_NODES, Hive.HDFS_NAME_SERVICES, Hive.HDFS_HADOOP_USER, Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER);
        
        // database
        DataSystemResourceDefinition databaseDefinition = hiveDefinition.getChildren().get(DataSystemResourceType.HIVE_DATABASE);
        Assertions.assertThat(databaseDefinition).isNotNull();
        
        // table
        DataSystemResourceDefinition tableDefinition = databaseDefinition.getChildren().get(DataSystemResourceType.HIVE_TABLE);
        Assertions.assertThat(tableDefinition).isNotNull();
    }
}
