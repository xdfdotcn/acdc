package cn.xdf.acdc.connect.transforms.format.numberic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class CastRecordValueDecimalTest {

    private final CastRecordValueDecimal transform = new CastRecordValueDecimal();

    @Before
    public void setup() {
        transform.configure(new HashMap<>());
    }
    
    @Test
    public void testDefaultDecimalFormatIsDouble() {
        SinkRecord result = transform.apply(fakeFlatFieldsSinkRecordWithoutToHandleFieldType());
        
        Assert.assertEquals(22.1111, ((Struct) result.value()).get("field_decimal_1"));
        Assert.assertEquals(11.22, ((Struct) result.value()).get("field_decimal_2"));
    }
    
    @Test
    public void testDoubleDecimalFormatIsApplied() {
        Map<String, String> config = new HashMap<>();
        config.put("decimal.format", "double");
        transform.configure(config);
        SinkRecord result = transform.apply(fakeFlatFieldsSinkRecordWithoutToHandleFieldType());
        
        Assert.assertEquals(22.1111, ((Struct) result.value()).get("field_decimal_1"));
        Assert.assertEquals(11.22, ((Struct) result.value()).get("field_decimal_2"));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testErrorFormatConfigShouldThrowException() {
        Map<String, String> config = new HashMap<>();
        config.put("decimal.format", "error_config");
        transform.configure(config);
        transform.apply(fakeFlatFieldsSinkRecordWithoutToHandleFieldType());
    }
    
    @Test
    public void testStringDecimalFormatIsApplied() {
        Map<String, String> config = new HashMap<>();
        config.put("decimal.format", "string");
        transform.configure(config);
        SinkRecord result = transform.apply(fakeFlatFieldsSinkRecordWithoutToHandleFieldType());
        
        Assert.assertEquals("22.1111", ((Struct) result.value()).get("field_decimal_1"));
        Assert.assertEquals("11.22", ((Struct) result.value()).get("field_decimal_2"));
    }

    private SinkRecord fakeFlatFieldsSinkRecordWithoutToHandleFieldType() {
        Struct value = getValueWithoutToHandleFieldType();
        Struct key = getKey();
        return new SinkRecord("test-topic", 0, key.schema(), key, value.schema(), value, 0);
    }

    private Struct getValueWithoutToHandleFieldType() {
        Schema decimalSchema = SchemaBuilder.bytes().name("org.apache.kafka.connect.data.Decimal").build();
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
                .field("field_decimal_1", decimalSchema)
                .field("field_decimal_2", decimalSchema)
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
        value.put("field_decimal_1", new BigDecimal("22.1111"));
        value.put("field_decimal_2", new BigDecimal("11.22"));
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
}
