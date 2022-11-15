package cn.xdf.acdc.connect.plugins.converter.json;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;

public class SchemalessJsonConverterTest {

    private StringDeserializer deserializer = new StringDeserializer();

    @Test
    public void testFromConnectDataShouldConverterDataAsExpectedWithBasicField() {
        String topic = "mysql_project1_db1_tb1";
        SinkRecord sinkRecord = fakeBasicFieldSinkRecord(topic);

        SchemalessJsonConverter keyConverter = new SchemalessJsonConverter();
        keyConverter.configure(new HashMap<>(), true);
        byte[] keyBytes = keyConverter.fromConnectData(topic, sinkRecord.keySchema(), sinkRecord.key());
        String actualKeyString = deserializer.deserialize(topic, keyBytes);
        Assert.assertEquals("{\"id\":1}", actualKeyString);

        SchemalessJsonConverter valueConverter = new SchemalessJsonConverter();
        valueConverter.configure(new HashMap<>(), false);
        byte[] valueBytes = valueConverter.fromConnectData(topic, sinkRecord.valueSchema(), sinkRecord.value());
        String actualValueString = deserializer.deserialize(topic, valueBytes);
        Assert.assertEquals("{"
                        + "\"id\":1,"
                        + "\"name\":\"record-name\","
                        + "\"__op\":\"u\","
                        + "\"__table\":\"source_table\","
                        + "\"field_int16\":4,"
                        + "\"field_int32\":5,"
                        + "\"field_int64\":6,"
                        + "\"field_float32\":1.1,"
                        + "\"field_float64\":2.2,"
                        + "\"field_boolean\":true,"
                        + "\"field_string\":\"string_\","
                        + "\"field_bytes\":\"AQE=\""
                        + "}",
                actualValueString);
    }

    @Test
    public void testFromConnectDataShouldConverterDataAsExpectedWithAdvancedField() {
        String topic = "mysql_project1_db1_tb1";
        SinkRecord sinkRecord = fakeAdvancedFieldSinkRecord(topic);

        SchemalessJsonConverter keyConverter = new SchemalessJsonConverter();
        keyConverter.configure(new HashMap<>(), true);
        byte[] keyBytes = keyConverter.fromConnectData(topic, sinkRecord.keySchema(), sinkRecord.key());
        String actualKeyString = deserializer.deserialize(topic, keyBytes);
        Assert.assertEquals("{\"id\":1}", actualKeyString);

        SchemalessJsonConverter valueConverter = new SchemalessJsonConverter();
        valueConverter.configure(new HashMap<>(), false);
        byte[] valueBytes = valueConverter.fromConnectData(topic, sinkRecord.valueSchema(), sinkRecord.value());
        String actualValueString = deserializer.deserialize(topic, valueBytes);
        Assert.assertEquals("{"
                        + "\"id\":1,"
                        + "\"__op\":\"u\","
                        + "\"__table\":\"source_table\","
                        + "\"datetime\":\"2022-08-11 00:00:00.000\","
                        + "\"date\":\"2022-08-11\","
                        + "\"time\":\"00:00:00\","
                        + "\"zone_timestamp\":\"2022-09-11 08:00:00.000\""
                        + "}",
                actualValueString);
    }

    private SinkRecord fakeBasicFieldSinkRecord(final String topic) {
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("__op", Schema.STRING_SCHEMA)
                .field("__table", Schema.STRING_SCHEMA)
                .field("field_int16", Schema.INT16_SCHEMA)
                .field("field_int32", Schema.INT32_SCHEMA)
                .field("field_int64", Schema.INT64_SCHEMA)
                .field("field_float32", Schema.FLOAT32_SCHEMA)
                .field("field_float64", Schema.FLOAT64_SCHEMA)
                .field("field_boolean", Schema.BOOLEAN_SCHEMA)
                .field("field_string", Schema.STRING_SCHEMA)
                .field("field_bytes", Schema.BYTES_SCHEMA)
                .build();
        Struct key = new Struct(keySchema);
        key.put("id", 1L);

        Struct value = new Struct(valueSchema);
        value.put("id", 1L);
        value.put("name", "record-name");
        value.put("__op", "u");
        value.put("__table", "source_table");
        value.put("field_int16", (short) 4);
        value.put("field_int32", 5);
        value.put("field_int64", 6L);
        value.put("field_float32", 1.1f);
        value.put("field_float64", 2.2);
        value.put("field_boolean", true);
        value.put("field_string", "string_");
        byte[] bytes = {1, 1};
        value.put("field_bytes", ByteBuffer.wrap(bytes));
        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 0);
    }

    private SinkRecord fakeAdvancedFieldSinkRecord(final String topic) {
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        Schema datetimeSchema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Timestamp").build();
        Schema dateSchema = SchemaBuilder.int32().name("org.apache.kafka.connect.data.Date").build();
        Schema timeSchema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Time").build();
        Schema zoneTimestampSchema = SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("__op", Schema.STRING_SCHEMA)
                .field("__table", Schema.STRING_SCHEMA)
                .field("datetime", datetimeSchema)
                .field("date", dateSchema)
                .field("time", timeSchema)
                .field("zone_timestamp", zoneTimestampSchema)
                .build();

        Struct key = new Struct(keySchema);
        key.put("id", 1L);

        Struct value = new Struct(valueSchema);
        value.put("id", 1L);
        value.put("__op", "u");
        value.put("__table", "source_table");

        value.put("datetime", getDateTime());
        value.put("date", getDateTime());
        value.put("time", getDateTime());
        value.put("zone_timestamp", "2022-09-11T00:00:00Z");
        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 0);
    }

    private Date getDateTime() {
        // 1. just like datetime/date/time in source mysql DB
        LocalDateTime localDateTime = LocalDateTime.parse("2022-08-11T00:00:00");
        // 2. like date in debezium
        return Date.from(localDateTime.atZone(ZoneId.of("GMT")).toInstant());
    }

}
