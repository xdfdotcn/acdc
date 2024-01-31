package cn.xdf.acdc.devops.service.process.connection.columnconfiguration;

import com.google.common.collect.Sets;

import java.util.Set;

public class ConnectionColumnConfigurationConstant {
    
    public static final String META_OP = "__op";
    
    public static final String META_OP_TYPE = "String";
    
    public static final String META_KAFKA_RECORD_OFFSET = "__kafka_record_offset";
    
    public static final String META_KAFKA_RECORD_OFFSET_TYPE = "long";
    
    public static final String META_LOGICAL_DEL = "__logical_del";
    
    public static final String META_LOGICAL_DELETION = "__deleted";
    
    public static final String META_LOGICAL_DEL_TYPE = "Integer";
    
    public static final String META_DATE_TIME = "__datetime";
    
    public static final String META_DATE_TIME_TYPE = "String";
    
    public static final String META_DATE_TIME_VALUE = "${datetime}";
    
    public static final Set<String> META_FIELD_FILTER_SET = Sets.newHashSet(
            META_LOGICAL_DEL,
            META_DATE_TIME
    );
    
    public static final Set<String> META_FIELD_SET = Sets.newHashSet(
            META_OP,
            META_KAFKA_RECORD_OFFSET,
            META_LOGICAL_DEL,
            META_DATE_TIME
    );
}
