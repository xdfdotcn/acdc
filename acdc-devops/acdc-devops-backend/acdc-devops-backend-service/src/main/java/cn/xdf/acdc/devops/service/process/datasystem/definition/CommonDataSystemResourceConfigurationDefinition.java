package cn.xdf.acdc.devops.service.process.datasystem.definition;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.MetadataSourceType;

public class CommonDataSystemResourceConfigurationDefinition {
    
    public static class Authorization {
        
        public static final String USERNAME_NAME = "username";
        
        public static final String USERNAME_DESC = "数据库用户名";
        
        public static final String PASSWORD_NAME = "password";
        
        public static final String PASSWORD_DESC = "数据库密码";
        
        public static final ConfigurationDefinition<String> USERNAME = new ConfigurationDefinition(
                false,
                false,
                USERNAME_NAME,
                USERNAME_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);
        
        public static final ConfigurationDefinition<String> PASSWORD = new ConfigurationDefinition(
                true,
                false,
                PASSWORD_NAME,
                PASSWORD_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);
    }
    
    public static class Endpoint {
        
        public static final String HOST_NAME = "host";
        
        public static final String HOST_DESC = "host";
        
        public static final String PORT_NAME = "port";
        
        public static final String PORT_DESC = "port";
        
        public static final ConfigurationDefinition<String> HOST = new ConfigurationDefinition(
                false,
                false,
                HOST_NAME,
                HOST_DESC,
                SystemConstant.EMPTY_STRING,
                ConfigurationValueType.STRING,
                new String[0], value -> true);
        
        public static final ConfigurationDefinition<Integer> PORT = new ConfigurationDefinition(
                false,
                false,
                PORT_NAME,
                PORT_DESC,
                0,
                ConfigurationValueType.INTEGER, new Integer[0], value -> true);
    }
    
    public static class Internal {
        
        public static final String SOURCE_NAME = "source";
        
        public static final String SOURCE_DESC = "metadata source";
        
        public static final ConfigurationDefinition<MetadataSourceType> SOURCE = new ConfigurationDefinition(
                true,
                false,
                SOURCE_NAME,
                SOURCE_DESC,
                MetadataSourceType.FROM_PANDORA,
                ConfigurationValueType.ENUM,
                MetadataSourceType.values(), value -> true);
    }
}
