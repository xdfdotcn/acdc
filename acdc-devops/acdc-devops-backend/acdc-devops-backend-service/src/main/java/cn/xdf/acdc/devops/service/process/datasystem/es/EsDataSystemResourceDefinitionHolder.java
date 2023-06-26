package cn.xdf.acdc.devops.service.process.datasystem.es;

import java.util.HashMap;
import java.util.Map;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.es.EsDataSystemResourceConfigurationDefinition.Cluster;

public final class EsDataSystemResourceDefinitionHolder {

    private static final DataSystemResourceDefinition ES_DATA_SYSTEM_RESOURCE_DEFINITION = generateEsDataSystemResourceDefinition();

    private EsDataSystemResourceDefinitionHolder() {
    }

    private static DataSystemResourceDefinition generateEsDataSystemResourceDefinition() {
        return generateClusterDefinition();
    }

    private static DataSystemResourceDefinition generateClusterDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        children.put(DataSystemResourceType.ELASTIC_SEARCH_INDEX, generateIndexDefinition());

        return new DataSystemResourceDefinition(
                generateClusterConfigurations(),
                DataSystemResourceType.ELASTIC_SEARCH_CLUSTER,
                false,
                true,
                false,
                children
        );
    }

    private static DataSystemResourceDefinition generateIndexDefinition() {
        return new DataSystemResourceDefinition(generateIndexConfigurations(), DataSystemResourceType.ELASTIC_SEARCH_INDEX, true, false, true);
    }

    private static Map<String, ConfigurationDefinition<?>> generateIndexConfigurations() {
        final Map<String, ConfigurationDefinition<?>> configurationDefinitions = new HashMap<>();
        return configurationDefinitions;
    }

    private static Map<String, ConfigurationDefinition<?>> generateClusterConfigurations() {
        final Map<String, ConfigurationDefinition<?>> configurationDefinitions = new HashMap<>();
        configurationDefinitions.put(Cluster.USERNAME.getName(), Cluster.USERNAME);
        configurationDefinitions.put(Cluster.PASSWORD.getName(), Cluster.PASSWORD);
        configurationDefinitions.put(Cluster.NODE_SERVERS.getName(), Cluster.NODE_SERVERS);
        return configurationDefinitions;
    }

    /**
     * Get data system definition for elastic cluster.
     *
     * @return elastic cluster resource definition
     */
    public static DataSystemResourceDefinition get() {
        return ES_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
