package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.tidb.TidbDataSystemResourceConfigurationDefinition.Server;

import java.util.HashMap;
import java.util.Map;

public final class TidbDataSystemResourceDefinitionHolder {

    private static final DataSystemResourceDefinition TIDB_DATA_SYSTEM_RESOURCE_DEFINITION = generateTidbDataSystemResourceDefinition();

    private TidbDataSystemResourceDefinitionHolder() {
    }

    private static DataSystemResourceDefinition generateTidbDataSystemResourceDefinition() {
        return generateTidbClusterDataSystemResourceDefinition();
    }

    private static DataSystemResourceDefinition generateTidbClusterDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();

        children.put(DataSystemResourceType.TIDB_SERVER, generateInstanceDataSystemResourceDefinition());
        children.put(DataSystemResourceType.TIDB_DATABASE, generateDatabaseDataSystemResourceDefinition());

        return new DataSystemResourceDefinition(generateClusterConfiguration(), DataSystemResourceType.TIDB_CLUSTER, false, true, false, children);
    }

    private static DataSystemResourceDefinition generateInstanceDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(generateServerConfiguration(), DataSystemResourceType.TIDB_SERVER, false, false, false);
    }

    private static Map<String, ConfigurationDefinition<?>> generateServerConfiguration() {
        Map<String, ConfigurationDefinition<?>> configuration = new HashMap<>();

        configuration.put(Server.HOST.getName(), Server.HOST);
        configuration.put(Server.PORT.getName(), Server.PORT);

        return configuration;
    }

    private static DataSystemResourceDefinition generateDatabaseDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();

        children.put(DataSystemResourceType.TIDB_TABLE, generateTableDataSystemResourceDefinition());

        return new DataSystemResourceDefinition(DataSystemResourceType.TIDB_DATABASE, true, true, false, children);
    }

    private static DataSystemResourceDefinition generateTableDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(DataSystemResourceType.TIDB_TABLE, true, false, true);
    }

    private static Map<String, ConfigurationDefinition<?>> generateClusterConfiguration() {
        Map<String, ConfigurationDefinition<?>> configuration = new HashMap<>();

        configuration.put(Cluster.USERNAME.getName(), Cluster.USERNAME);
        configuration.put(Cluster.PASSWORD.getName(), Cluster.PASSWORD);
        configuration.put(Cluster.SOURCE.getName(), Cluster.SOURCE);

        return configuration;
    }

    /**
     * Get data system definition for tidb cluster.
     *
     * @return tidb cluster resource definition
     */
    public static DataSystemResourceDefinition get() {
        return TIDB_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
