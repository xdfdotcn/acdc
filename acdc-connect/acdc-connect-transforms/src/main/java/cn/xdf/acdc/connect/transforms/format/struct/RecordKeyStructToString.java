package cn.xdf.acdc.connect.transforms.format.struct;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordKeyStructToString implements Transformation<SinkRecord> {
    
    public static final String DELIMITER_NAME = "delimiter";
    
    public static final String DEFAULT_DELIMITER = "-";
    
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DELIMITER_NAME, ConfigDef.Type.STRING, DEFAULT_DELIMITER, ConfigDef.Importance.MEDIUM,
                    "A delimiter concat multi key struct fields, and the default is hyphen(-).");
    
    private String delimiter;
    
    @Override
    public SinkRecord apply(final SinkRecord record) {
        final Struct keyStruct = (Struct) record.key();
        final List<String> stringKeys = keyStruct.schema().fields().stream().map(field -> keyStruct.get(field).toString()).collect(Collectors.toList());
        final String newKey = String.join(delimiter, stringKeys);
        return new SinkRecord(record.topic(), record.kafkaPartition(), Schema.STRING_SCHEMA, newKey, record.valueSchema(),
                record.value(), record.kafkaOffset(), record.timestamp(), record.timestampType(), record.headers());
    }
    
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    
    @Override
    public void close() {
    
    }
    
    @Override
    public void configure(final Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        delimiter = config.getString(DELIMITER_NAME);
    }
}
