package cn.xdf.acdc.devops.service.process.datasystem.tidb;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Endpoint;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Internal;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;

public class TidbDataSystemResourceConfigurationDefinition {
    
    public static class Cluster {
        
        public static final ConfigurationDefinition<String> USERNAME = Authorization.USERNAME;
        
        public static final ConfigurationDefinition<String> PASSWORD = Authorization.PASSWORD;
        
        public static final ConfigurationDefinition<MetadataSourceType> SOURCE = Internal.SOURCE;
    }
    
    public static class Server {
        
        public static final ConfigurationDefinition<String> HOST = Endpoint.HOST;
        
        public static final ConfigurationDefinition<Integer> PORT = Endpoint.PORT;
    }
}
