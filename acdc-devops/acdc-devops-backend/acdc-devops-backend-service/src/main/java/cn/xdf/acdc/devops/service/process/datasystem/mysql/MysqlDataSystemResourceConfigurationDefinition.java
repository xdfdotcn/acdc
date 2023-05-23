package cn.xdf.acdc.devops.service.process.datasystem.mysql;

import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Authorization;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Endpoint;
import cn.xdf.acdc.devops.service.process.datasystem.definition.CommonDataSystemResourceConfigurationDefinition.Internal;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationDefinition;
import cn.xdf.acdc.devops.service.process.datasystem.definition.ConfigurationValueType;

public class MysqlDataSystemResourceConfigurationDefinition {
    
    public static class Cluster {
        
        public static final ConfigurationDefinition<String> USERNAME = Authorization.USERNAME;
        
        public static final ConfigurationDefinition<String> PASSWORD = Authorization.PASSWORD;
        
        public static final ConfigurationDefinition<MetadataSourceType> SOURCE = Internal.SOURCE;
    }
    
    public static class Instance {
        
        public static final String MYSQL_INSTANCE_ROLE_NAME = "role_type";
        
        public static final String MYSQL_INSTANCE_ROLE_DESC = "实例身份";
        
        public static final ConfigurationDefinition<String> HOST = Endpoint.HOST;
        
        public static final ConfigurationDefinition<Integer> PORT = Endpoint.PORT;
        
        public static final ConfigurationDefinition<MysqlInstanceRoleType> ROLE_TYPE = new ConfigurationDefinition(
                false,
                false,
                MYSQL_INSTANCE_ROLE_NAME,
                MYSQL_INSTANCE_ROLE_DESC,
                MysqlInstanceRoleType.MASTER,
                ConfigurationValueType.ENUM,
                MysqlInstanceRoleType.values(), value -> true
        );
    }
}
