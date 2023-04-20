package cn.xdf.acdc.connect.transforms.valuemapper;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

public class StringValueMapperTest {

    @Test
    public void testApplyShouldMapperSuccessWithConfigurationValue() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("field", "__op");
        configMap.put("mappings", "c:I,u:U,d:D");
        StringValueMapper<SinkRecord> stringValueMapper = new StringValueMapper<>();
        stringValueMapper.configure(configMap);

        SinkRecord sinkRecord = createSinkRecord("c");
        SinkRecord applySinkRecord = stringValueMapper.apply(sinkRecord);
        Struct sinkStruct = (Struct) sinkRecord.value();
        Struct applySinkStruct = (Struct) applySinkRecord.value();

        Assert.assertEquals(sinkRecord.valueSchema(), sinkRecord.valueSchema());

        Assert.assertEquals(String.valueOf(sinkStruct.get("int8_schema")), String.valueOf(applySinkStruct.get("int8_schema")));
        Assert.assertEquals(String.valueOf(sinkStruct.get("int16_schema")), String.valueOf(applySinkStruct.get("int16_schema")));
        Assert.assertEquals(String.valueOf(applySinkStruct.get("__op")), "I");

        sinkRecord = createSinkRecord("u");
        applySinkRecord = stringValueMapper.apply(sinkRecord);
        applySinkStruct = (Struct) applySinkRecord.value();
        Assert.assertEquals(String.valueOf(applySinkStruct.get("__op")), "U");

        sinkRecord = createSinkRecord("d");
        applySinkRecord = stringValueMapper.apply(sinkRecord);
        applySinkStruct = (Struct) applySinkRecord.value();
        Assert.assertEquals(String.valueOf(applySinkStruct.get("__op")), "D");
    }

    @Test
    public void testApplyShouldUseFieldOriginalValueWhenNotMapper() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("field", "__op");
        configMap.put("mappings", "c:I,u:U,d:D");
        StringValueMapper<SinkRecord> stringValueMapper = new StringValueMapper<>();
        stringValueMapper.configure(configMap);

        SinkRecord sinkRecord = createSinkRecord("QTest");
        SinkRecord applySinkRecord = stringValueMapper.apply(sinkRecord);
        Struct applySinkStruct = (Struct) applySinkRecord.value();
        Assert.assertEquals(String.valueOf(applySinkStruct.get("__op")), "QTest");
    }

    @Test(expected = ConfigException.class)
    public void testApplyShouldThrownExceptionWithFieldTypeNotString() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("field", "int8_schema");
        configMap.put("mappings", "c:I,u:U,d:D");
        StringValueMapper<SinkRecord> stringValueMapper = new StringValueMapper<>();
        stringValueMapper.configure(configMap);

        SinkRecord sinkRecord = createSinkRecord("c");
        stringValueMapper.apply(sinkRecord);
    }

    private SinkRecord createSinkRecord(final String op) {
        Schema recordSchema = SchemaBuilder.struct().name("record").version(2)
            .field("int8_schema", Schema.INT8_SCHEMA)
            .field("int16_schema", Schema.INT16_SCHEMA)
            .field("__op", Schema.STRING_SCHEMA)
            .build();
        Struct struct = new Struct(recordSchema);
        struct
            .put("int8_schema", (byte) 42)
            .put("int16_schema", (short) 42)
            .put("__op", op);
        SinkRecord sinkRecord = new SinkRecord(
            "test-topic",
            12,
            Schema.STRING_SCHEMA,
            null,
            recordSchema,
            struct,
            0L,
            0L,
            TimestampType.CREATE_TIME
        );
        return sinkRecord;
    }
}
