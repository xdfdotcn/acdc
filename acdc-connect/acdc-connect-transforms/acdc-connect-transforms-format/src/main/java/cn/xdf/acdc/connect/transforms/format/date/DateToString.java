package cn.xdf.acdc.connect.transforms.format.date;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The transform only can be used in sink connectors for which to keep some SinkRecord properties(like kafkaOffset and timestampType) in the record,
 * and we design this transform apply to sink connector use only in our scenario.
 */
public class DateToString implements Transformation<SinkRecord> {

    public static final String NOT_BEEN_EXTRACTED_SCHEMA_NAME_SUFFIX = "Envelope";

    public static final String ZONED_TIMESTAMP_LOGICAL_NAME = "io.debezium.time.ZonedTimestamp";

    public static final String ZONED_TIMESTAMP_FORMATTER_NAME = "zoned.timestamp.formatter";

    public static final String ZONED_TIMESTAMP_FORMATTER_ZONED = "zoned";

    public static final String ZONED_TIMESTAMP_FORMATTER_LOCAL = "local";

    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final DateTimeFormatter ZONED_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS'Z'");

    private static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter DEFAULT_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ZONED_TIMESTAMP_FORMATTER_NAME, ConfigDef.Type.STRING, ZONED_TIMESTAMP_FORMATTER_ZONED, ConfigDef.Importance.MEDIUM,
                    "A supported zoned.timestamp.formatter is zoned (yyyy-MM-dd HH:mm:ss.SSS'Z') or local (yyyy-MM-dd HH:mm:ss.SSS), and default zoned");

    private static final String ENVELOPE_BEFORE_NAME = "before";

    private static final String ENVELOPE_AFTER_NAME = "after";

    public static final Set<String> TO_TRANSFER_SCHEMA_NAMES = new HashSet<String>() {{
            add(Date.LOGICAL_NAME);
            add(Time.LOGICAL_NAME);
            add(Timestamp.LOGICAL_NAME);
            add(ZONED_TIMESTAMP_LOGICAL_NAME);
        }};

    private String zonedTimestampFormatter;

    @Override
    public SinkRecord apply(final SinkRecord record) {
        // 墓碑消息
        if (record.valueSchema() == null) {
            return record;
        }

        if (record.valueSchema().name().endsWith(NOT_BEEN_EXTRACTED_SCHEMA_NAME_SUFFIX)) {
            Schema oldEnvelopedValueSchema = record.valueSchema();
            Struct oldEnvelopedValue = (Struct) record.value();
            Field before = oldEnvelopedValueSchema.field(ENVELOPE_BEFORE_NAME);
            Field after = oldEnvelopedValueSchema.field(ENVELOPE_AFTER_NAME);

            boolean beforeNeedTransfer = needTransferOrNot(before.schema());
            boolean afterNeedTransfer = needTransferOrNot(after.schema());
            if (!beforeNeedTransfer && !afterNeedTransfer) {
                return record;
            }

            Schema beforeSchema = before.schema();
            Struct beforeStruct = (Struct) oldEnvelopedValue.get(before);
            Struct handledBeforeStruct = beforeNeedTransfer ? getNewValue(beforeSchema, beforeStruct) : beforeStruct;

            Schema afterSchema = after.schema();
            Struct afterStruct = (Struct) oldEnvelopedValue.get(after);
            Struct handledAfterStruct = afterNeedTransfer ? getNewValue(afterSchema, afterStruct) : afterStruct;

            Struct newEnvelopedValue = getNewEnvelopedValue(oldEnvelopedValue, handledBeforeStruct, handledAfterStruct);
            return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newEnvelopedValue.schema(),
                    newEnvelopedValue, record.kafkaOffset(), record.timestamp(), record.timestampType(), record.headers());
        } else {
            Schema oldValueSchema = record.valueSchema();
            if (needTransferOrNot(oldValueSchema)) {
                Struct newValue = getNewValue(oldValueSchema, (Struct) record.value());
                return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValue.schema(),
                        newValue, record.kafkaOffset(), record.timestamp(), record.timestampType(), record.headers());
            }
            return record;
        }
    }

    private Struct getNewEnvelopedValue(final Struct oldEnvelopedValue, final Struct handledBeforeStruct, final Struct handledAfterStruct) {
        Schema oldEnvelopedValueSchema = oldEnvelopedValue.schema();
        Schema newEnvelopedValueSchema = getNewEnvelopedValueSchema(handledBeforeStruct, handledAfterStruct, oldEnvelopedValueSchema);
        Struct newEnvelopedValue = new Struct(newEnvelopedValueSchema);
        oldEnvelopedValueSchema.fields().forEach(field -> {
            switch (field.name()) {
                case ENVELOPE_BEFORE_NAME:
                    newEnvelopedValue.put(field.name(), handledBeforeStruct);
                    break;
                case ENVELOPE_AFTER_NAME:
                    newEnvelopedValue.put(field.name(), handledAfterStruct);
                    break;
                default:
                    newEnvelopedValue.put(field.name(), oldEnvelopedValue.get(field.name()));
                    break;
            }
        });
        return newEnvelopedValue;
    }

    private Schema getNewEnvelopedValueSchema(final Struct handledBeforeStruct, final Struct handledAfterStruct, final Schema oldEnvelopedValueSchema) {
        SchemaBuilder newEnvelopedValueSchemaBuilder = SchemaBuilder.struct().name(oldEnvelopedValueSchema.name()).version(oldEnvelopedValueSchema.version());
        oldEnvelopedValueSchema.fields().forEach(field -> {
            switch (field.name()) {
                case ENVELOPE_BEFORE_NAME:
                    if (Objects.isNull(handledBeforeStruct)) {
                        newEnvelopedValueSchemaBuilder.field(ENVELOPE_BEFORE_NAME, oldEnvelopedValueSchema.field(ENVELOPE_BEFORE_NAME).schema());
                    } else {
                        newEnvelopedValueSchemaBuilder.field(ENVELOPE_BEFORE_NAME, handledBeforeStruct.schema());
                    }
                    break;
                case ENVELOPE_AFTER_NAME:
                    if (Objects.isNull(handledAfterStruct)) {
                        newEnvelopedValueSchemaBuilder.field(ENVELOPE_AFTER_NAME, oldEnvelopedValueSchema.field(ENVELOPE_AFTER_NAME).schema());
                    } else {
                        newEnvelopedValueSchemaBuilder.field(ENVELOPE_AFTER_NAME, handledAfterStruct.schema());
                    }
                    break;
                default:
                    newEnvelopedValueSchemaBuilder.field(field.name(), field.schema());
                    break;
            }
        });
        return newEnvelopedValueSchemaBuilder.build();
    }

    private Struct getNewValue(final Schema oldValueSchema, final Struct oldValue) {
        if (Objects.isNull(oldValue)) {
            return null;
        }
        Schema newValueSchema = getNewValueSchema(oldValueSchema);
        Struct newValue = new Struct(newValueSchema);

        for (Field field : oldValueSchema.fields()) {
            Object newFieldValue = getNewFieldValue(oldValue.get(field), field);
            newValue.put(field.name(), newFieldValue);
        }
        return newValue;
    }

    private Object getNewFieldValue(final Object oldFieldValue, final Field field) {
        if (Objects.isNull(field.schema().name()) || Objects.isNull(oldFieldValue)) {
            return oldFieldValue;
        }
        switch (field.schema().name()) {
            case Date.LOGICAL_NAME:
                LocalDateTime date = ((java.util.Date) oldFieldValue).toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
                return date.format(DEFAULT_DATE_FORMATTER);
            case Timestamp.LOGICAL_NAME:
                LocalDateTime dateTime = ((java.util.Date) oldFieldValue).toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
                return dateTime.format(DEFAULT_DATETIME_FORMATTER);
            case Time.LOGICAL_NAME:
                LocalDateTime time = ((java.util.Date) oldFieldValue).toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
                return time.format(DEFAULT_TIME_FORMATTER);
            case ZONED_TIMESTAMP_LOGICAL_NAME:
                ZonedDateTime parsedZonedDateTime = ZonedDateTime.parse((String) oldFieldValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                return getZonedDataTimeString(parsedZonedDateTime);
            default:
                return oldFieldValue;
        }
    }

    private Schema getNewValueSchema(final Schema oldValueSchema) {
        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(oldValueSchema.name()).version(oldValueSchema.version());
        for (Field field : oldValueSchema.fields()) {
            Schema newFieldSchema = getNewFieldSchema(field.schema());
            valueSchemaBuilder.field(field.name(), newFieldSchema);
        }
        return valueSchemaBuilder.build();
    }

    private static Schema getNewFieldSchema(final Schema oldFieldSchema) {
        if (Objects.isNull(oldFieldSchema.name())) {
            return oldFieldSchema;
        }
        switch (oldFieldSchema.name()) {
            case Date.LOGICAL_NAME:
            case Time.LOGICAL_NAME:
            case Timestamp.LOGICAL_NAME:
            case ZONED_TIMESTAMP_LOGICAL_NAME:
                if (oldFieldSchema.isOptional()) {
                    return SchemaBuilder.string().optional().build();
                } else {
                    return SchemaBuilder.string().build();
                }
            default:
                return oldFieldSchema;
        }
    }

    private String getZonedDataTimeString(final ZonedDateTime zonedDateTime) {
        switch (zonedTimestampFormatter) {
            case ZONED_TIMESTAMP_FORMATTER_ZONED:
                return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC)
                        .format(ZONED_TIMESTAMP_FORMATTER);
            case ZONED_TIMESTAMP_FORMATTER_LOCAL:
                return zonedDateTime.withZoneSameInstant(ZoneId.systemDefault())
                        .format(DEFAULT_DATETIME_FORMATTER);
            default:
                throw new UnsupportedOperationException("Unknown zoned.timestamp.formatter: " + zonedTimestampFormatter);
        }
    }

    private boolean needTransferOrNot(final Schema valueSchema) {
        if (valueSchema.fields() == null) {
            return false;
        }
        return valueSchema.fields().stream().anyMatch(field -> TO_TRANSFER_SCHEMA_NAMES.contains(field.schema().name()));
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
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        zonedTimestampFormatter = config.getString(ZONED_TIMESTAMP_FORMATTER_NAME);
    }
}
