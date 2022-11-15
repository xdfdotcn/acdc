package cn.xdf.acdc.connect.kafka.sink.utils;

import java.util.HashMap;
import java.util.Map;

public class KafkaSinkTestUtil {

    /**
     * Fake kafka sink task config.
     *
     * @return kafka sink task config
     */
    public static Map<String, String> fakeKafkaSinkTaskConfig() {
        Map<String, String> prop = new HashMap<>();
        prop.put("connector.class", "cn.xdf.acdc.connect.kafka.KafkaSinkConnector");
        prop.put("destinations.mysql_kafka_sink.fields.mapping", "d:id,school_id:school_id,name:name,field_bool:field_bool,"
                + "field_datetime:field_datetime,field_timestamp:field_timestamp,field_date:field_date,field_time:field_time,"
                + "field_text:field_text,field_decimal:field_decimal,field_double:field_double,field_float:field_float,"
                + "field_json:field_json,field_blob:field_blob,field_char:field_char,field_bit:field_bit,field_nvarchar:field_nvarchar,"
                + "__op:__op,__table:__table,__deleted:__deleted");
        prop.put("tasks.max", "1");
        prop.put("destinations.mysql_kafka_sink.fields.whitelist", "id,school_id,name,field_bool,field_datetime,"
                + "field_timestamp,field_date,field_time,field_text,field_decimal,field_double,field_float,field_json,"
                + "field_blob,field_char,field_bit,field_nvarchar,__op,__table,__deleted");
        prop.put("destinations.mysql_kafka_sink.delete.mode", "NONE");
        prop.put("destinations.mysql_kafka_sink.exclude.meta.fields", "false");
        prop.put("topics", "source-mysql-CDC-dataline-debezium_source-city");
        prop.put("destinations", "mysql_kafka_sink");
        prop.put("db.timezone", "Asia/Shanghai");
        prop.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        prop.put("value.converter.schema.registry.url", "http://127.0.0.1:8081");
        prop.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        prop.put("key.converter.schema.registry.url", "http://127.0.0.1:8081");
        prop.put("sink.kafka.bootstrap.servers", "localhost:9092");
        prop.put("sink.kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"xxx\" password=\"xxx\";");
        prop.put("sink.kafka.security.protocol", "SASL_PLAINTEXT");
        prop.put("sink.kafka.sasl.mechanism", "SCRAM-SHA-512");
        return prop;
    }
}
