package cn.xdf.acdc.devops.service.process.datasystem.hive;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveDataSystemResourceConfigurationDefinition.Hive;

import java.util.HashMap;
import java.util.Map;

public final class HiveDataSystemResourceDefinitionHolder {
    
    private static final DataSystemResourceDefinition HIVE_DATA_SYSTEM_RESOURCE_DEFINITION = generateHiveDataSystemResourceDefinition();
    
    private HiveDataSystemResourceDefinitionHolder() {
    }
    
    private static DataSystemResourceDefinition generateHiveDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        children.put(DataSystemResourceType.HIVE_DATABASE, generateDatabaseDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(generateHiveConfiguration(), DataSystemResourceType.HIVE, false, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateDatabaseDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        children.put(DataSystemResourceType.HIVE_TABLE, generateTableDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(DataSystemResourceType.HIVE_DATABASE, true, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateTableDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(DataSystemResourceType.HIVE_TABLE, true, false, true);
    }
    
    private static Map<String, ConfigurationDefinition<?>> generateHiveConfiguration() {
        Map<String, ConfigurationDefinition<?>> configuration = new HashMap<>();
        
        configuration.put(Hive.HIVE_METASTORE_URIS.getName(), Hive.HIVE_METASTORE_URIS);
        configuration.put(Hive.HDFS_NAME_SERVICES.getName(), Hive.HDFS_NAME_SERVICES);
        configuration.put(Hive.HDFS_NAME_NODES.getName(), Hive.HDFS_NAME_NODES);
        configuration.put(Hive.HDFS_HADOOP_USER.getName(), Hive.HDFS_HADOOP_USER);
        configuration.put(Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER.getName(), Hive.HDFS_CLIENT_FAILOVER_PROXY_PROVIDER);
        
        return configuration;
    }
    
    /**
     * Get data system definition for hive.
     *
     * @return hive resource definition
     */
    public static DataSystemResourceDefinition get() {
        return HIVE_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
