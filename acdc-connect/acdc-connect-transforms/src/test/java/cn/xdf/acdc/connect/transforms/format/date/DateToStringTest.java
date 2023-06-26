package cn.xdf.acdc.connect.transforms.format.date;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateToStringTest {

    private final DateToString transform = new DateToString();

    @Before
    public void setup() {
        transform.configure(new HashMap<>());
    }

    @Test
    public void testZonedTimestampFormatterShouldUseZonedFormatWithDefaultOrZonedConfig() {
        SinkRecord result = transform.apply(fakeEnvelopedValueSinkRecordWithToHandleFieldType());
        Assert.assertEquals("2022-09-11 00:00:00.000Z", ((Struct) ((Struct) result.value()).get("before")).get("zone_timestamp"));
        Assert.assertEquals("2022-09-11 00:00:00.000Z", ((Struct) ((Struct) result.value()).get("after")).get("zone_timestamp"));
    
        Map<String, String> config0 = new HashMap<>();
        config0.put("zoned.timestamp.formatter", "zoned");
        transform.configure(config0);
        SinkRecord configuredResult = transform.apply(fakeEnvelopedValueSinkRecordWithToHandleFieldType());
        Assert.assertEquals("2022-09-11 00:00:00.000Z", ((Struct) ((Struct) configuredResult.value()).get("before")).get("zone_timestamp"));
        Assert.assertEquals("2022-09-11 00:00:00.000Z", ((Struct) ((Struct) configuredResult.value()).get("after")).get("zone_timestamp"));
    }

    @Test
    public void testZonedTimestampFormatterShouldUseLocalFormatWithLocalConfig() {
        Map<String, String> config0 = new HashMap<>();
        config0.put("zoned.timestamp.formatter", "local");
        transform.configure(config0);
        SinkRecord configuredResult = transform.apply(fakeEnvelopedValueSinkRecordWithToHandleFieldType());
        Assert.assertEquals("2022-09-11 08:00:00.000", ((Struct) ((Struct) configuredResult.value()).get("before")).get("zone_timestamp"));
        Assert.assertEquals("2022-09-11 08:00:00.000", ((Struct) ((Struct) configuredResult.value()).get("after")).get("zone_timestamp"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testZonedTimestampFormatterShouldThrowExceptionsWithUnknownConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("zoned.timestamp.formatter", "unknown");
        transform.configure(config);
        transform.apply(fakeEnvelopedValueSinkRecordWithToHandleFieldType());
    }

    @Test
    public void testApplyShouldReturnTheSameOneWithoutToHandleFieldType() {
        SinkRecord sinkRecord = fakeEnvelopedValueSinkRecordWithoutToHandleFieldType();
        SinkRecord result = transform.apply(sinkRecord);
        Assert.assertSame(sinkRecord, result);
    }

    @Test
    public void testApplyToBeforeShouldReturnAdapterRecordWithToHandleFieldType() {
        SinkRecord sinkRecord = fakeEnvelopedValueSinkRecordWithToHandleFieldType();
        SinkRecord result = transform.apply(sinkRecord);
        Assert.assertEquals(sinkRecord.kafkaOffset(), result.kafkaOffset());
        Assert.assertEquals(sinkRecord.timestampType(), result.timestampType());
        Assert.assertEquals(sinkRecord.topic(), result.topic());
        Assert.assertEquals(sinkRecord.kafkaPartition(), result.kafkaPartition());
        Assert.assertEquals(sinkRecord.key(), result.key());
        Struct oldValue = (Struct) ((Struct) sinkRecord.value()).get("before");
        Struct newValue = (Struct) ((Struct) result.value()).get("before");
        Assert.assertEquals(oldValue.get("id"), newValue.get("id"));
        Assert.assertEquals(oldValue.get("__op"), newValue.get("__op"));
        Assert.assertEquals(oldValue.get("__table"), newValue.get("__table"));
        Assert.assertEquals("2022-08-11 00:00:00.000", newValue.get("datetime"));
        Assert.assertEquals("2022-08-11", newValue.get("date"));
        Assert.assertEquals("00:00:00", newValue.get("time"));
        Assert.assertEquals("2022-09-11 00:00:00.000Z", newValue.get("zone_timestamp"));

        Schema oldSchema = sinkRecord.valueSchema().field("before").schema();
        Schema resultSchema = result.valueSchema().field("before").schema();
        Assert.assertEquals(oldSchema.field("id"), resultSchema.field("id"));
        Assert.assertEquals(oldSchema.field("__op"), resultSchema.field("__op"));
        Assert.assertEquals(oldSchema.field("__table"), resultSchema.field("__table"));
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("datetime").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("date").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("time").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("zone_timestamp").schema());
    }

    @Test
    public void testApplyToAfterShouldReturnAdapterRecordWithToHandleFieldType() {
        SinkRecord sinkRecord = fakeEnvelopedValueSinkRecordWithToHandleFieldType();
        SinkRecord result = transform.apply(sinkRecord);
        Assert.assertEquals(sinkRecord.kafkaOffset(), result.kafkaOffset());
        Assert.assertEquals(sinkRecord.timestampType(), result.timestampType());
        Assert.assertEquals(sinkRecord.topic(), result.topic());
        Assert.assertEquals(sinkRecord.kafkaPartition(), result.kafkaPartition());
        Assert.assertEquals(sinkRecord.key(), result.key());
        Struct oldValue = (Struct) ((Struct) sinkRecord.value()).get("after");
        Struct newValue = (Struct) ((Struct) result.value()).get("after");
        Assert.assertEquals(oldValue.get("id"), newValue.get("id"));
        Assert.assertEquals(oldValue.get("__op"), newValue.get("__op"));
        Assert.assertEquals(oldValue.get("__table"), newValue.get("__table"));
        Assert.assertEquals("2022-08-11 00:00:00.000", newValue.get("datetime"));
        Assert.assertEquals("2022-08-11", newValue.get("date"));
        Assert.assertEquals("00:00:00", newValue.get("time"));
        Assert.assertEquals("2022-09-11 00:00:00.000Z", newValue.get("zone_timestamp"));

        Schema oldSchema = sinkRecord.valueSchema().field("after").schema();
        Schema resultSchema = result.valueSchema().field("after").schema();
        Assert.assertEquals(oldSchema.field("id"), resultSchema.field("id"));
        Assert.assertEquals(oldSchema.field("__op"), resultSchema.field("__op"));
        Assert.assertEquals(oldSchema.field("__table"), resultSchema.field("__table"));
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("datetime").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("date").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("time").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, resultSchema.field("zone_timestamp").schema());
    }

    private Struct getValueWithoutToHandleFieldType() {
        Schema valueSchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Value")
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
        return value;
    }

    private Struct getValueWithToHandleFieldType() {
        Schema datetimeSchema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Timestamp").build();
        Schema dateSchema = SchemaBuilder.int32().name("org.apache.kafka.connect.data.Date").build();
        Schema timeSchema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Time").build();
        Schema zoneTimestampSchema = SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").build();
        Schema valueSchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("__op", Schema.STRING_SCHEMA)
                .field("__table", Schema.STRING_SCHEMA)
                .field("datetime", datetimeSchema)
                .field("date", dateSchema)
                .field("time", timeSchema)
                .field("zone_timestamp", zoneTimestampSchema)
                .build();
        Struct value = new Struct(valueSchema);
        value.put("id", 1L);
        value.put("__op", "u");
        value.put("__table", "source_table");

        value.put("datetime", getDateTime());
        value.put("date", getDateTime());
        value.put("time", getDateTime());
        value.put("zone_timestamp", "2022-09-11T00:00:00Z");
        return value;
    }

    private Struct getKey() {
        Schema keySchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Key")
                .field("id", Schema.INT64_SCHEMA)
                .build();

        Struct key = new Struct(keySchema);
        key.put("id", 1L);
        return key;
    }

    private SinkRecord fakeEnvelopedValueSinkRecordWithoutToHandleFieldType() {
        Struct key = getKey();
    
        Struct value = getEnvelopedValue();
        return new SinkRecord("test-topic", 0, key.schema(), key, value.schema(), value, 5, 1679902739906L, TimestampType.CREATE_TIME);
    }
    
    private Struct getEnvelopedValue() {
        Schema sourceSchema = SchemaBuilder.struct().name("cn.xdf.acdc.connector.tidb.Source").field("table", Schema.STRING_SCHEMA).build();
        Schema transactionSchema = SchemaBuilder.struct().build();
        Schema valueSchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Envelope")
                .version(2)
                .field("before", getValueWithoutToHandleFieldType().schema())
                .field("after", getValueWithoutToHandleFieldType().schema())
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("transaction", transactionSchema)
                .build();
        Struct source = new Struct(sourceSchema);
        source.put("table", "test-table");
        Struct value = new Struct(valueSchema);
        
        value.put("before", getValueWithoutToHandleFieldType());
        value.put("after", getValueWithoutToHandleFieldType());
        value.put("source", source);
        value.put("op", "u");
        value.put("ts_ms", 1679902739922L);
        value.put("transaction", new Struct(transactionSchema));
        return value;
    }
    
    private SinkRecord fakeEnvelopedValueSinkRecordWithToHandleFieldType() {
        Struct key = getKey();
    
        Struct value = getValue();
        return new SinkRecord("test-topic", 0, key.schema(), key, value.schema(), value, 5, 1679902739906L, TimestampType.CREATE_TIME);
    }
    
    private Struct getValue() {
        Schema sourceSchema = SchemaBuilder.struct().name("cn.xdf.acdc.connector.tidb.Source").field("table", Schema.STRING_SCHEMA).build();
        Schema transactionSchema = SchemaBuilder.struct().build();
        Schema valueSchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Envelope")
                .version(2)
                .field("before", getValueWithToHandleFieldType().schema())
                .field("after", getValueWithToHandleFieldType().schema())
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("transaction", transactionSchema)
                .build();
        Struct source = new Struct(sourceSchema);
        source.put("table", "test-table");
        Struct value = new Struct(valueSchema);
        
        value.put("before", getValueWithToHandleFieldType());
        value.put("after", getValueWithToHandleFieldType());
        value.put("source", source);
        value.put("op", "u");
        value.put("ts_ms", 1679902739922L);
        value.put("transaction", new Struct(transactionSchema));
        return value;
    }
    
    private Date getDateTime() {
        // 1. just like datetime/date/time in source mysql DB
        LocalDateTime localDateTime = LocalDateTime.parse("2022-08-11T00:00:00");
        // 2. like date in debezium
        return Date.from(localDateTime.atZone(ZoneId.of("GMT")).toInstant());
    }
}
