package cn.xdf.acdc.devops.service.error;

public final class ErrorMsg {
    
    public static final String E_101 = "The requested resource does not exist or has been deleted";
    
    public static final String E_102 = "The resources already exist";
    
    public static final String E_103 = "The mysql database instance is not configured. Please check the configuration";
    
    public static final String E_105 = "System internal error, Please contact the ACDC administrator.";
    
    public static final String E_107 = "The rdb instance does not exist";
    
    public static final String E_108 = "The cluster does not exist.";
    
    public static final String E_109 = "The kafka cluster does not exist.";
    
    public static final String E_110 = "The connection does not exist.";
    
    public static final String E_111 = "Request parameter error.";
    
    public static final String E_112 = "The kafka serializer is incorrectly configured.";
    
    public class Authorization {
        
        public static final String INSUFFICIENT_PERMISSIONS = "insufficient permissions";
    }
    
    public class DataSystem {
        
        public static final String UNEXPECTED_CONFIGURATION_VALUE = "unexpected configuration value, name: %s, expected: %s, actual: %s";
    }
}
