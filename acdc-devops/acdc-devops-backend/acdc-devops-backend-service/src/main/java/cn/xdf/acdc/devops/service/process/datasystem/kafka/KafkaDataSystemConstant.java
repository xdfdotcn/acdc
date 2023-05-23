package cn.xdf.acdc.devops.service.process.datasystem.kafka;

import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaDataSystemConstant {
    
    public static class Connector {
        
        public static class Sink {
            
            public static class Configuration {
                
                public static final Set<String> SENSITIVE_CONFIGURATION_NAMES = Sets.newHashSet("sink.kafka.sasl.jaas.config");
                
                public static final String KAFKA_CONFIG_PREFIX = "sink.kafka.";
                
                public static final Map<String, String> JSON_DATA_FORMAT_CONFIGURATION = new HashMap<>();
                
                // configuration for json data format
                static {
                    JSON_DATA_FORMAT_CONFIGURATION.put("transforms", "tombstoneFilter,dateToString");
                    
                    // filter tombstone record
                    JSON_DATA_FORMAT_CONFIGURATION.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
                    JSON_DATA_FORMAT_CONFIGURATION.put("transforms.tombstoneFilter.predicate", "isTombstone");
                    
                    // date to string smt
                    JSON_DATA_FORMAT_CONFIGURATION.put("transforms.dateToString.type", "cn.xdf.acdc.connect.transforms.format.date.DateToString");
                    JSON_DATA_FORMAT_CONFIGURATION.put("transforms.dateToString.zoned.timestamp.formatter", "zoned");
                    
                    // predicate
                    JSON_DATA_FORMAT_CONFIGURATION.put("predicates", "isTombstone");
                    JSON_DATA_FORMAT_CONFIGURATION.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
                    
                    // converter
                    JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter", "org.apache.kafka.connect.json.JsonConverter");
                    JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter.schemas.enable", "false");
                    JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter.decimal.format", "NUMERIC");
                    JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter", "org.apache.kafka.connect.json.JsonConverter");
                    JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter.schemas.enable", "false");
                    JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter.decimal.format", "NUMERIC");
                }
                
                public static final Map<String, String> CDC_V1_DATA_FORMAT_CONFIGURATION = new HashMap<>();
                
                // configuration for cdc v1 data format
                static {
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("transforms", "tombstoneFilter,unwrap");
                    
                    // filter tombstone record
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("transforms.tombstoneFilter.predicate", "isTombstone");
                    
                    // unwrap record from debezium json
                    // we should only use this smt for records which has not been extracted
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("transforms.unwrap.delete.handling.mode", "rewrite");
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("transforms.unwrap.add.fields", "op,table");
                    
                    // predicate
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("predicates", "isTombstone");
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
                    
                    // converter
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter", "cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter");
                    CDC_V1_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter", "cn.xdf.acdc.connect.plugins.converter.xdf.XdfRecordConverter");
                }
                
                public static final Map<String, String> SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION = new HashMap<>();
                
                // configuration for schema less json format
                static {
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms", "tombstoneFilter,dateToString,unwrap");
                    
                    // filter tombstone record
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.tombstoneFilter.type", "org.apache.kafka.connect.transforms.Filter");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.tombstoneFilter.predicate", "isTombstone");
                    
                    // date to string smt
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.dateToString.type", "cn.xdf.acdc.connect.transforms.format.date.DateToString");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.dateToString.zoned.timestamp.formatter", "local");
                    
                    // unwrap record from debezium json
                    // we should only use this smt for records which has not been extracted
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.unwrap.delete.handling.mode", "rewrite");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("transforms.unwrap.add.fields", "op,table");
                    
                    // predicate
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("predicates", "isTombstone");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("predicates.isTombstone.type", "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone");
                    
                    // converter
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter", "org.apache.kafka.connect.json.JsonConverter");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter.schemas.enable", "false");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.key.converter.decimal.format", "NUMERIC");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter", "org.apache.kafka.connect.json.JsonConverter");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter.schemas.enable", "false");
                    SCHEMA_LESS_JSON_DATA_FORMAT_CONFIGURATION.put("sink.kafka.value.converter.decimal.format", "NUMERIC");
                }
            }
        }
    }
}
