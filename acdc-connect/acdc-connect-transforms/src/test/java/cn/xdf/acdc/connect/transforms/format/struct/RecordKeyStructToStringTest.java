package cn.xdf.acdc.connect.transforms.format.struct;

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

public class RecordKeyStructToStringTest {
    
    private final RecordKeyStructToString transform = new RecordKeyStructToString();
    
    @Before
    public void setup() {
        transform.configure(new HashMap<>());
    }
    
    @Test
    public void testSingleKeyFieldBeCastToString() {
        SinkRecord result = transform.apply(fakeSingleKeyFieldRecord());
        
        Assert.assertEquals("1", result.key());
    }
    
    @Test
    public void testMultiKeyFieldsShouldBeCastToStringWithDefaultConfig() {
        SinkRecord result = transform.apply(fakeMultiKeyFieldsRecord());
        
        Assert.assertEquals("100-this_one", result.key());
    }
    
    @Test
    public void testMultiKeyFieldsShouldBeCastToStringWithSpecificConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("delimiter", "|");
        transform.configure(config);
        SinkRecord result = transform.apply(fakeMultiKeyFieldsRecord());
        
        Assert.assertEquals("100|this_one", result.key());
    }
    
    private SinkRecord fakeMultiKeyFieldsRecord() {
        Struct value = getValueWithoutToHandleFieldType();
        Struct key = getMultiFieldsKey();
        return new SinkRecord("test-topic", 0, key.schema(), key, value.schema(), value, 0);
    }
    
    private SinkRecord fakeSingleKeyFieldRecord() {
        Struct value = getValueWithoutToHandleFieldType();
        Struct key = getSingleFieldKey();
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
        return value;
    }
    
    private Struct getSingleFieldKey() {
        Schema keySchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Key")
                .field("id", Schema.INT64_SCHEMA)
                .build();
        
        Struct key = new Struct(keySchema);
        key.put("id", 1L);
        return key;
    }
    
    private Struct getMultiFieldsKey() {
        Schema keySchema = SchemaBuilder.struct()
                .name("source_tidb_acdc_source_tidb_cluster_origin_acdc_source_tidb.acdc_source_tidb.city.Key")
                .field("code", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        
        Struct key = new Struct(keySchema);
        key.put("code", 100L);
        key.put("name", "this_one");
        return key;
    }
}
