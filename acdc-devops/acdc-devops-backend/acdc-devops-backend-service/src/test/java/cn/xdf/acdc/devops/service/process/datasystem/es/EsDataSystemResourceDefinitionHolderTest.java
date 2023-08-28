package cn.xdf.acdc.devops.service.process.datasystem.es;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.es.EsDataSystemResourceConfigurationDefinition.Cluster;

@RunWith(MockitoJUnitRunner.class)
class EsDataSystemResourceDefinitionHolderTest {

    @Test
    public void testGet() {
        DataSystemResourceDefinition esDefinition = EsDataSystemResourceDefinitionHolder.get();

        // cluster
        Assertions.assertThat(esDefinition.getType())
                .isEqualTo(DataSystemResourceType.ELASTICSEARCH_CLUSTER);
        Assertions.assertThat(esDefinition.getChildren()).isNotEmpty();
        Assertions.assertThat(esDefinition.isDataCollection()).isFalse();
        Assertions.assertThat(esDefinition.isHasDataCollectionChild()).isTrue();

        // cluster configuration
        Assertions.assertThat(esDefinition.getConfigurationDefinitions())
                .containsValues(Cluster.USERNAME, Cluster.PASSWORD, Cluster.NODE_SERVERS);

        // index
        DataSystemResourceDefinition indexDefinition = esDefinition
                .getChildren().get(DataSystemResourceType.ELASTICSEARCH_INDEX);
        Assertions.assertThat(indexDefinition.getChildren()).isEmpty();
        Assertions.assertThat(indexDefinition.isDataCollection()).isTrue();
        Assertions.assertThat(indexDefinition.isDynamic()).isTrue();
        Assertions.assertThat(indexDefinition.isHasDataCollectionChild()).isFalse();
    }
}
