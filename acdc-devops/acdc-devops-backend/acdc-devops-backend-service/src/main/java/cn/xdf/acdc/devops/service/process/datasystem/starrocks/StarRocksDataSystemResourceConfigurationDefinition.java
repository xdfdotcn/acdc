package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Endpoint;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Internal;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationValueType;

public class StarRocksDataSystemResourceConfigurationDefinition {
    
    public static class Cluster {
        
        public static final ConfigurationDefinition<String> USERNAME = Authorization.USERNAME;
        
        public static final ConfigurationDefinition<String> PASSWORD = Authorization.PASSWORD;
        
        public static final ConfigurationDefinition<MetadataSourceType> SOURCE = Internal.SOURCE;
    }
    
    public static class FrontEnd {
        
        public static final ConfigurationDefinition<String> HOST = Endpoint.HOST;
    
        public static final String JDBC_PORT_NAME = "jdbcPort";
    
        public static final String JDBC_PORT_DESC = "jdbcPort";
    
        public static final String HTTP_PORT_NAME = "httpPort";
    
        public static final String HTTP_PORT_DESC = "httpPort";
        
        public static final ConfigurationDefinition<Integer> JDBC_PORT = new ConfigurationDefinition(
                false,
                false,
                JDBC_PORT_NAME,
                JDBC_PORT_DESC,
                0,
                ConfigurationValueType.INTEGER, new Integer[0], value -> true);
        
        public static final ConfigurationDefinition<Integer> HTTP_PORT = new ConfigurationDefinition(
                false,
                false,
                HTTP_PORT_NAME,
                HTTP_PORT_DESC,
                0,
                ConfigurationValueType.INTEGER, new Integer[0], value -> true);
    }
}
