package cn.xdf.acdc.connect.core.sink.filter;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.core.util.config.DestinationConfig;
import cn.xdf.acdc.connect.core.util.config.SinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class ConditionsFilterTest {

    private static final String ERROR_EXPRESS = " date_value <> 2 ";

    private Schema zonedTimestampSchema = SchemaBuilder.string().name(ZonedTimestamp.LOGICAL_NAME).build();

    private Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("int_value", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool_value", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short_value", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte_value", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long_value", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float_value", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double_value", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("date_value", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("time_value", org.apache.kafka.connect.data.Time.SCHEMA)
            .field("datetime_value", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .field("timestamp_value", zonedTimestampSchema)
            .build();

    private Struct struct = new Struct(schema)
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool_value", true)
            .put("short_value", (short) 10)
            .put("byte_value", (byte) 10)
            .put("long_value", 10L)
            .put("float_value", (float) 10.0)
            .put("double_value", (double) 10.0)
            .put("int_value", 10)
            // 2021-08-11 00:00:00 -> 2021-08-11 08:00:00
            .put("date_value", org.apache.kafka.connect.data.Date.toLogical(org.apache.kafka.connect.data.Date.SCHEMA, 18850))
            // 1970-01-01 10:18:18 -> 1970-01-01 18:18:18
            .put("time_value", Time.toLogical(Time.SCHEMA, 37098000))
            // 2021-08-11 10:18:18 -> 2021-08-11 18:18:18
            .put("datetime_value", Timestamp.toLogical(Timestamp.SCHEMA, 1628677098000L))
            .put("timestamp_value", "2021-08-11T10:18:18Z");

    private SinkRecord sinkRecord = new SinkRecord("test_topic", 1, null, null, schema, struct, 42);

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test(expected = ConnectException.class)
    public void shouldBeSetSpaceBeforeAndAfterFieldName() {
        getSingleRecordFilter("firstName=='Alex'").filter(sinkRecord);
    }

    @Test
    public void shouldPassFilter() {
        Assert.assertEquals(true, getSingleRecordFilter(SinkConfig.STRING_DEFAULT).filter(sinkRecord));
    }

    @Test(expected = Exception.class)
    public void testErrorExpress() {
        getSingleRecordFilter(ERROR_EXPRESS).filter(sinkRecord);
    }

    @Test
    public void testEqual() {
        Assert.assertEquals(true, getSingleRecordFilter("firstName == 'Alex'").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("bool_value == true").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("short_value == 10").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("byte_value == 10").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("long_value == 10").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("float_value == 10").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("double_value == 10").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("int_value == 10").filter(sinkRecord));
    }

    @Test
    public void testNotEqual() {
        Assert.assertEquals(false, getSingleRecordFilter("firstName == 'Alexs'").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("bool_value == false").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("short_value == 11").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("byte_value == 11").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("long_value == 11").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("float_value == 11").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("double_value == 11").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("int_value == 11").filter(sinkRecord));
    }

    @Test
    public void testAndOr() {
        Assert.assertEquals(false, getSingleRecordFilter("bool_value and firstName == 'Alexs'").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("bool_value or firstName == 'Alexs'").filter(sinkRecord));
        Assert.assertEquals(false, getSingleRecordFilter("(bool_value and firstName == 'Alexs') and (bool_value or firstName == 'Alexs')").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("(bool_value and firstName == 'Alexs') or (bool_value or firstName == 'Alexs')").filter(sinkRecord));
    }

    @Test
    public void testDateCompare() {
        Assert.assertEquals(true, getSingleRecordFilter("datetime_value == '2021-08-11 10:18:18' ").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("time_value == '1970-01-01 10:18:18' ").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("date_value == '2021-08-11 00:00:00' ").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("timestamp_value == '2021-08-11 18:18:18' ").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("timestamp_value > '2021-07-11 18:18:18' ").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("timestamp_value < '2021-09-11 18:18:18' ").filter(sinkRecord));
        Assert.assertEquals(true, getSingleRecordFilter("timestamp_value > '2021-08-10 20:18:18' ").filter(sinkRecord));

    }

    protected Filter getSingleRecordFilter(final String s) {
        DestinationConfig destinationConfig = new DestinationConfig(null, null, null, null, s, null, null, null, null);
        return new ConditionsFilter(destinationConfig, TimeZone.getTimeZone("Asia/Shanghai"));
    }

}
