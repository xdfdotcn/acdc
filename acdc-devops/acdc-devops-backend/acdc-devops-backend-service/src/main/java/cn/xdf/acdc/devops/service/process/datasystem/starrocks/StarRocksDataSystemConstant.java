package cn.xdf.acdc.devops.service.process.datasystem.starrocks;

import com.google.common.collect.Sets;

import java.util.Set;

public class StarRocksDataSystemConstant {
    
    public static class Metadata {
        
        public static class StarRocks {
            
            public static final String PK_INDEX_NAME = "PRIMARY_KEY";
            
            public static final Set<String> SYSTEM_DATABASES = Sets.newHashSet("information_schema", "_statistics_");
        }
    }
    
    static class Connector {
        
        static class Sink {
            
            static class Configuration {
                
                public static final Set<String> SENSITIVE_CONFIGURATION_NAMES = Sets.newHashSet(Configuration.PASSWORD);
                
                public static final String TOPICS = "topics";
                
                public static final String DATABASE = "database.name";
                
                public static final String TABLE = "table.name";
                
                public static final String LOAD_URL = "load.url";
                
                public static final String USERNAME = "username";
    
                public static final String PASSWORD = "password";
                
                public static final String TRANSFORMS = "transforms";
    
                public static final String TRANSFORMS_VALUE = "dateToString,unwrap,valueMapperSource,replaceField";
    
                public static final String TRANSFORMS_DATE_TO_STRING_TYPE = "transforms.dateToString.type";
    
                public static final String TRANSFORMS_DATE_TO_STRING_TYPE_VALUE = "cn.xdf.acdc.connect.transforms.format.date.DateToString";
    
                public static final String TRANSFORMS_DATE_TO_STRING_ZONED_TIMESTAMP_FORMATTER = "transforms.dateToString.zoned.timestamp.formatter";
    
                public static final String TRANSFORMS_DATE_TO_STRING_ZONED_TIMESTAMP_FORMATTER_VALUE = "local";
                
                public static final String TRANSFORMS_UNWRAP_TYPE = "transforms.unwrap.type";
                
                public static final String TRANSFORMS_UNWRAP_TYPE_VALUE = "io.debezium.transforms.ExtractNewRecordState";
    
                public static final String TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE = "transforms.unwrap.delete.handling.mode";
    
                public static final String TRANSFORMS_UNWRAP_DELETE_HANDLING_MODE_VALUE = "rewrite";
                
                public static final String TRANSFORMS_UNWRAP_ADD_FIELDS = "transforms.unwrap.add.fields";
                
                public static final String TRANSFORMS_UNWRAP_ADD_FIELDS_VALUE = "op";
                
                public static final String TRANSFORMS_VALUE_MAPPER_SOURCE_TYPE = "transforms.valueMapperSource.type";

                public static final String TRANSFORMS_VALUE_MAPPER_SOURCE_TYPE_VALUE = "cn.xdf.acdc.connect.transforms.valuemapper.StringValueMapper";

                public static final String TRANSFORMS_VALUE_MAPPER_SOURCE_MAPPINGS = "transforms.valueMapperSource.mappings";

                public static final String TRANSFORMS_VALUE_MAPPER_SOURCE_MAPPINGS_VALUE = "c:0,u:0,d:1";

                public static final String TRANSFORMS_TRANSFORMS_VALUE_MAPPER_SOURCE_FIELD = "transforms.valueMapperSource.field";

                public static final String TRANSFORMS_TRANSFORMS_VALUE_MAPPER_SOURCE_FIELD_VALUE = "__op";
    
                public static final String TRANSFORMS_REPLACE_FIELD_TYPE = "transforms.replaceField.type";
                
                public static final String TRANSFORMS_REPLACE_FIELD_TYPE_VALUE = "org.apache.kafka.connect.transforms.ReplaceField$Value";
                
                public static final String TRANSFORMS_REPLACE_FIELD_WHITELIST = "transforms.replaceField.whitelist";
    
                public static final String TRANSFORMS_REPLACE_FIELD_RENAME = "transforms.replaceField.renames";
    
                public static final String SINK_COLUMNS = "sink.columns";
            }
        }
    }
}
