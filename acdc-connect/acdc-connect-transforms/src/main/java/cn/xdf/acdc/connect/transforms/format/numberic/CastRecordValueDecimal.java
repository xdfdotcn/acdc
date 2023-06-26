package cn.xdf.acdc.connect.transforms.format.numberic;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CastRecordValueDecimal implements Transformation<SinkRecord> {
    
    public static final String DECIMAL_FORMAT_NAME = "decimal.format";
    
    public static final String DECIMAL_FORMAT_DOUBLE = "double";
    
    public static final String DECIMAL_FORMAT_STRING = "string";
    
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DECIMAL_FORMAT_NAME, ConfigDef.Type.STRING, DECIMAL_FORMAT_DOUBLE, ConfigDef.Importance.MEDIUM,
                    "A supported decimal.format is double (lossy) or string (not lossy), and default is double.");
    
    private String decimalFormat;
    
    @Override
    public SinkRecord apply(final SinkRecord record) {
        // 墓碑消息
        if (record.valueSchema() == null || record.value() == null) {
            return record;
        }
        Optional<Field> decimalFieldOptional = record.valueSchema().fields().stream()
                .filter(field -> Objects.equals(Decimal.LOGICAL_NAME, field.schema().name())).findAny();
        if (!decimalFieldOptional.isPresent()) {
            return record;
        }
        Schema updatedSchema = getUpdatedSchema(record.valueSchema());
        Struct updatedValue = getUpdatedValue(updatedSchema, (Struct) record.value());
        
        return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema,
                updatedValue, record.kafkaOffset(), record.timestamp(), record.timestampType(), record.headers());
    }
    
    private Struct getUpdatedValue(final Schema updatedSchema, final Struct value) {
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            Object fieldValue = value.get(field);
            if (Objects.equals(Decimal.LOGICAL_NAME, field.schema().name())) {
                fieldValue = castValueToSpecificType(fieldValue);
            }
            updatedValue.put(updatedSchema.field(field.name()), fieldValue);
        }
        return updatedValue;
    }
    
    private Schema getUpdatedSchema(final Schema valueSchema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            if (Objects.equals(Decimal.LOGICAL_NAME, field.schema().name())) {
                SchemaBuilder fieldSchemaBuilder = getFieldType();
                if (field.schema().isOptional()) {
                    fieldSchemaBuilder.optional();
                }
                if (field.schema().defaultValue() != null) {
                    Schema fieldSchema = field.schema();
                    fieldSchemaBuilder.defaultValue(castValueToSpecificType(fieldSchema.defaultValue()));
                }
                builder.field(field.name(), fieldSchemaBuilder.build());
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }
    
    private Object castValueToSpecificType(final Object value) {
        if (value == null) {
            return null;
        }
        switch (decimalFormat) {
            case DECIMAL_FORMAT_DOUBLE:
                return Double.parseDouble(value.toString());
            case DECIMAL_FORMAT_STRING:
                return value.toString();
            default:
                return null;
        }
    }
    
    private SchemaBuilder getFieldType() {
        switch (decimalFormat) {
            case DECIMAL_FORMAT_DOUBLE:
                return SchemaBuilder.float64();
            case DECIMAL_FORMAT_STRING:
                return SchemaBuilder.string();
            default:
                throw new UnsupportedOperationException("Unknown decimal.format: " + decimalFormat);
        }
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
        decimalFormat = config.getString(DECIMAL_FORMAT_NAME);
    }
}
