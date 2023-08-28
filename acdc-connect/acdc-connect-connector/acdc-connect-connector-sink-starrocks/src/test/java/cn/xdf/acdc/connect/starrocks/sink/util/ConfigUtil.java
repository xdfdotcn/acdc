package cn.xdf.acdc.connect.starrocks.sink.util;

import java.util.HashMap;
import java.util.Map;

public class ConfigUtil {
    /**
     * Get sink configuration.
     *
     * @return the sink task configuration
     */
    public static Map<String, String> getSinkConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "cn.xdf.acdc.connect.starrocks.StarRocksSinkConnector");
        config.put("tasks.max", "1");
        config.put("topics", "source-mysql-14915-1_15_s_m-starrocks01");
        config.put("load.url", "localhost:8030");
        config.put("database.name", "db01");
        config.put("table.name", "tb01");
        config.put("username", "acdc");
        config.put("password", "acdc");
        config.put("sink.label.prefix", "_ACDC");
        config.put("sink.io.thread.count", "1");
        config.put("sink.scan.frequency.ms", "50");
        config.put("sink.buffer.flush.interval.ms", "1000");
        config.put("sink.buffer.flush.max.bytes", "67108864");
        config.put("sink.columns", "c3,id,c1,__op");
        config.put("transforms", "dateToString,unwrap,valueMapperSource,replaceField");
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.delete.handling.mode", "rewrite");
        config.put("transforms.unwrap.add.fields", "op,table");
        config.put("transforms.dateToString.type", "cn.xdf.acdc.connect.transforms.format.date.DateToString");
        config.put("transforms.dateToString.zoned.timestamp.formatter", "local");
        config.put("transforms.replaceField.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        config.put("transforms.replaceField.whitelist", "id,c1,c2,c3,__op");
        config.put("transforms.replaceField.renames", "id,id,c1,c1,c2,c2,c3,c3");
        config.put("transforms.valueMapperSource.type", "cn.xdf.acdc.connect.transforms.valuemapper.StringValueMapper");
        config.put("transforms.valueMapperSource.mappings", "c,0,u,0,d,1");
        config.put("transforms.valueMapperSource.field", "__op");
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", "http,//localhost:8081");
        config.put("value.converter.schema.registry.url", "http,//localhost:8081");
        
        return config;
    }
}
