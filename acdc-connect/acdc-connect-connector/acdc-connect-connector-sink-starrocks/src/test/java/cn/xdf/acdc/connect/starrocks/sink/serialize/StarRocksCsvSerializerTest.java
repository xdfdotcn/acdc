package cn.xdf.acdc.connect.starrocks.sink.serialize;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StarRocksCsvSerializerTest {
    
    private String SINK_CSV_COLUMN_SEPARATOR_DEFAULT = "\t";
    
    private List<String> columns = Lists.newArrayList("c1", "c2", "c3", "c4");
    
    @Test
    public void testSerialize() {
        StarRocksCsvSerializer serializer = new StarRocksCsvSerializer(
                SINK_CSV_COLUMN_SEPARATOR_DEFAULT,
                columns);
        
        SinkRecord sinkRecord = createSinkRecord();
        
        byte[] resultBytes = serializer.serialize(sinkRecord);
        
        String resultString = new String(resultBytes, StandardCharsets.UTF_8);
        
        List<String> values = Splitter.on(SINK_CSV_COLUMN_SEPARATOR_DEFAULT)
                .splitToList(resultString);
        
        /**
         * 0: "11"
         * 1: "\N"
         * 2: "\N"
         * 3: "test test01"
         */
        Assert.assertEquals("11", values.get(0));
        Assert.assertEquals("\\N", values.get(1));
        Assert.assertEquals("\\N", values.get(2));
        Assert.assertEquals("test  test01", values.get(3));
    }
    
    private SinkRecord createSinkRecord() {
        Schema schema = SchemaBuilder.struct().name("record").version(1)
                .field("c1", Schema.INT32_SCHEMA)
                .field("c2", SchemaBuilder.int64().optional().build())
                .field("c4", Schema.STRING_SCHEMA)
                .build();
        
        Struct record = new Struct(schema)
                .put("c1", 11)
                .put("c2", null)
                .put("c4", "test\t\ntest01");
        
        return new SinkRecord(
                "test-topic",
                1,
                Schema.STRING_SCHEMA,
                "key",
                schema,
                record,
                1L
        );
    }
}
