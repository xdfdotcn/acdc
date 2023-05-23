package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemResourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataSystemResourceDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Cluster;
import cn.xdf.acdc.devops.service.process.datasystem.mysql.MysqlDataSystemResourceConfigurationDefinition.Instance;

import java.util.HashMap;
import java.util.Map;

public final class MysqlDataSystemResourceDefinitionHolder {
    
    private static final DataSystemResourceDefinition MYSQL_DATA_SYSTEM_RESOURCE_DEFINITION = generateMysqlDataSystemResourceDefinition();
    
    private MysqlDataSystemResourceDefinitionHolder() {
    }
    
    private static DataSystemResourceDefinition generateMysqlDataSystemResourceDefinition() {
        return generateMysqlClusterDataSystemResourceDefinition();
    }
    
    private static DataSystemResourceDefinition generateMysqlClusterDataSystemResourceDefinition() {
        // cluster
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        children.put(DataSystemResourceType.MYSQL_INSTANCE, generateInstanceDataSystemResourceDefinition());
        children.put(DataSystemResourceType.MYSQL_DATABASE, generateIDatabaseDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(generateClusterConfigurations(), DataSystemResourceType.MYSQL_CLUSTER, false, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateInstanceDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(generateInstanceConfigurations(), DataSystemResourceType.MYSQL_INSTANCE, false, false, false);
    }
    
    private static Map<String, ConfigurationDefinition> generateInstanceConfigurations() {
        Map<String, ConfigurationDefinition> configurationDefinitions = new HashMap();
        configurationDefinitions.put(Instance.HOST.getName(), Instance.HOST);
        configurationDefinitions.put(Instance.PORT.getName(), Instance.PORT);
        configurationDefinitions.put(Instance.ROLE_TYPE.getName(), Instance.ROLE_TYPE);
        return configurationDefinitions;
    }
    
    private static DataSystemResourceDefinition generateIDatabaseDataSystemResourceDefinition() {
        Map<DataSystemResourceType, DataSystemResourceDefinition> children = new HashMap<>();
        children.put(DataSystemResourceType.MYSQL_TABLE, generateTableDataSystemResourceDefinition());
        
        return new DataSystemResourceDefinition(DataSystemResourceType.MYSQL_DATABASE, true, true, false, children);
    }
    
    private static DataSystemResourceDefinition generateTableDataSystemResourceDefinition() {
        return new DataSystemResourceDefinition(DataSystemResourceType.MYSQL_TABLE, true, false, true);
    }
    
    private static Map<String, ConfigurationDefinition> generateClusterConfigurations() {
        Map<String, ConfigurationDefinition> configurationDefinitions = new HashMap();
        configurationDefinitions.put(Cluster.USERNAME.getName(), Cluster.USERNAME);
        configurationDefinitions.put(Cluster.PASSWORD.getName(), Cluster.PASSWORD);
        configurationDefinitions.put(Cluster.SOURCE.getName(), Cluster.SOURCE);
        return configurationDefinitions;
    }
    
    /**
     * Get data system definition for mysql cluster.
     *
     * @return mysql cluster resource definition
     */
    public static DataSystemResourceDefinition get() {
        return MYSQL_DATA_SYSTEM_RESOURCE_DEFINITION;
    }
}
