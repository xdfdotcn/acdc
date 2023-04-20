package cn.xdf.acdc.connect.transforms.valuemapper;

import cn.xdf.acdc.connect.core.util.ConfigUtils;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class StringValueMapper<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "mapping value for the specified field name in record";

    private static final String COMMA = ",";

    private static final String COLON = ":";

    private static final String FIELD = "field";

    private static final String MAPPINGS = "mappings";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIELD,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.MEDIUM,
            "To be mapping field name."
        )
        .define(MAPPINGS,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.MEDIUM,
            "To config value mappings, eg: c:I,u:U,d:D"
        );

    private Map<String, String> valueMappings;

    private String mappingFieldName;

    @Override
    public void configure(final Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        valueMappings = ConfigUtils.stringToMap(config.originalsStrings(), MAPPINGS, COMMA, COLON);
        mappingFieldName = config.getString(FIELD);
    }

    @Override
    public R apply(final R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(final R record) {
        return record;
    }

    private R applyWithSchema(final R record) {
        Struct value = Requirements.requireStruct(record.value(), PURPOSE);
        Schema schema = value.schema();
        Struct updatedValue = new Struct(schema);

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Object fieldValue = value.get(fieldName);

            if (!Objects.equals(fieldName, mappingFieldName)) {
                updatedValue.put(fieldName, fieldValue);
            } else {
                updatedValue.put(fieldName, valueMappingOf(fieldValue));
            }
        }

        return newRecord(record, schema, updatedValue);
    }

    private Object valueMappingOf(final Object fieldValue) {
        Type inferredType = ConnectSchema.schemaType(fieldValue.getClass());
        if (Type.STRING != inferredType) {
            throw new ConfigException("Value mapping is only support for string type");
        }
        // not mapping return null
        Object mappingFieldValue = valueMappings.get(String.valueOf(fieldValue));
        return Objects.isNull(mappingFieldValue) ? fieldValue : mappingFieldValue;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // do nothing
    }

    private R newRecord(final R record, final Schema updatedSchema, final Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}
