package cn.xdf.acdc.connect.plugins.converter.json;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SchemalessJsonConverter extends JsonConverter {

    private static final String ZONED_TIMESTAMP_LOGIC_NAME = "io.debezium.time.ZonedTimestamp";

    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter DEFAULT_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Override
    public void configure(final Map<String, ?> configs) {
        Map<String, Object> conf = defaultSchemalessConfig(configs);
        super.configure(conf);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        Map<String, Object> conf = defaultSchemalessConfig(configs);
        super.configure(conf, isKey);
    }

    private Map<String, Object> defaultSchemalessConfig(final Map<String, ?> configs) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        conf.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        return conf;
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        if (!(value instanceof Struct)) {
            throw new UnsupportedOperationException();
        }

        Schema adaptedSchema = getAdaptedSchema(schema);
        Struct adaptedStructValue = getAdaptedStructValue(schema, (Struct) value, adaptedSchema);
        return super.fromConnectData(topic, adaptedSchema, adaptedStructValue);
    }

    private Schema getAdaptedSchema(final Schema baseSchema) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(baseSchema.name()).version(baseSchema.version());
        baseSchema.fields().forEach(field -> schemaBuilder.field(field.name(), adaptSchema(field.schema())));
        return schemaBuilder.build();
    }

    private Struct getAdaptedStructValue(final Schema baseSchema, final Struct baseStructValue, final Schema adaptedSchema) {
        Struct adaptedStructValue = new Struct(adaptedSchema);

        baseSchema.fields().forEach(field -> {
            Object adaptedValue = adaptValue(baseStructValue.get(field), field.schema());
            adaptedStructValue.put(field.name(), adaptedValue);
        });
        return adaptedStructValue;
    }

    private Schema adaptSchema(final Schema oldSchema) {
        if (Objects.isNull(oldSchema.name())) {
            return oldSchema;
        }

        switch (oldSchema.name()) {
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
            case Timestamp.LOGICAL_NAME:
            case Time.LOGICAL_NAME:
            case ZONED_TIMESTAMP_LOGIC_NAME:
                if (oldSchema.isOptional()) {
                    return SchemaBuilder.string().optional().build();
                }
                return SchemaBuilder.string().build();
            default:
                return oldSchema;
        }
    }

    private Object adaptValue(final Object oldValue, final Schema oldSchema) {
        if (Objects.isNull(oldSchema.name()) || Objects.isNull(oldValue)) {
            return oldValue;
        }

        switch (oldSchema.name()) {
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                LocalDateTime date = ((Date) oldValue).toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
                return date.format(DEFAULT_DATE_FORMATTER);
            case Timestamp.LOGICAL_NAME:
                LocalDateTime dateTime = ((Date) oldValue).toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
                return dateTime.format(DEFAULT_DATETIME_FORMATTER);
            case Time.LOGICAL_NAME:
                LocalDateTime time = ((Date) oldValue).toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
                return time.format(DEFAULT_TIME_FORMATTER);
            case ZONED_TIMESTAMP_LOGIC_NAME:
                ZonedDateTime parsedZonedDateTime = ZonedDateTime.parse((String) oldValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                ZonedDateTime zonedDateTime = parsedZonedDateTime.withZoneSameInstant(ZoneId.systemDefault());
                return zonedDateTime.format(DEFAULT_DATETIME_FORMATTER);
            default:
                return oldValue;
        }
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        throw new UnsupportedOperationException();
    }
}
