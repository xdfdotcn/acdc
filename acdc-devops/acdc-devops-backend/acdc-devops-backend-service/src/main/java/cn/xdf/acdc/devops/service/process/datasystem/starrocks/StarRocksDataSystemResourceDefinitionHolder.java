package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.starrocks.StarRocksDataSystemResourceConfigurationDefinition.FrontEnd;

import java.util.HashMap;
import java.util.Map;

public final class StarRocksDataSystemResourceDefinitionHolder {
    
    private static final DataSystemResourceDefinition STARROCKS_DATA_SYSTEM_RESOURCE_DEFINITION = generateStarRocksDataSystemResourceDefinition();
    
    private StarRocksDataSystemResourceDefinitionHolder() {
    }
    
    private static DataSystemResourceDefinition generateStarRocksDataSystemResourceDefinition() {
        return generateStarRocksClusterDataSystemResourceDefinition();
    }
    
    private static DataSystemResourceDefinition generateStarRocksClusterDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        
        children.put(DataSystemResourceType.STARROCKS_FRONTEND, generateFrontEndDataSystemResourceDefinition());
        children.put(DataSystemResourceType.STARROCKS_DATABASE, generateDatabaseDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(generateClusterConfiguration(), DataSystemResourceType.STARROCKS_CLUSTER, false, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateFrontEndDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(generateFrontEndConfiguration(), DataSystemResourceType.STARROCKS_FRONTEND, false, false, false);
    }
    
    private static Map<String, ConfigurationDefinition<?>> generateFrontEndConfiguration() {
        Map<String, ConfigurationDefinition<?>> configuration = new HashMap<>();
        
        configuration.put(FrontEnd.HOST.getName(), FrontEnd.HOST);
        configuration.put(FrontEnd.JDBC_PORT.getName(), FrontEnd.JDBC_PORT);
        
        return configuration;
    }
    
    private static DataSystemResourceDefinition generateDatabaseDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        
        children.put(DataSystemResourceType.STARROCKS_TABLE, generateTableDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(DataSystemResourceType.STARROCKS_DATABASE, true, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateTableDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(DataSystemResourceType.STARROCKS_TABLE, true, false, true);
    }
    
    private static Map<String, ConfigurationDefinition<?>> generateClusterConfiguration() {
        Map<String, ConfigurationDefinition<?>> configuration = new HashMap<>();
        
        configuration.put(Cluster.USERNAME.getName(), Cluster.USERNAME);
        configuration.put(Cluster.PASSWORD.getName(), Cluster.PASSWORD);
        configuration.put(Cluster.SOURCE.getName(), Cluster.SOURCE);
        
        return configuration;
    }
    
    /**
     * Get data system definition for starrocks cluster.
     *
     * @return starrocks cluster resource definition
     */
    public static DataSystemResourceDefinition get() {
        return STARROCKS_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
