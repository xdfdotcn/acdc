/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs.format.text;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.hdfs.common.Schemas;
import cn.xdf.acdc.connect.hdfs.format.metadata.SchemaProjector;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaInt;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaSmallint;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaString;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaTinyint;
import com.google.common.base.CharMatcher;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TextUtil.class})
@PowerMockIgnore("javax.management.*")
public class TextUtilTest {

    private SchemaProjector schemaProjector;

    private SimpleDateFormat dateFormat;

    private SimpleDateFormat timeFormat;

    private SimpleDateFormat timestampFormat;

    private java.util.Date now;

    private java.util.Date date;

    private java.util.Date time;

    private java.util.Date timestamp;

    private String timestampWithTimeZoneString;

    @Before
    public void setUp() throws Exception {
        schemaProjector = new SchemaProjector();
        SinkSchemaString sinkSchemaString = new SinkSchemaString();
        dateFormat = new SimpleDateFormat((String) PowerMock.field(SinkSchemaString.class, "DATE_FORMATTER").get(sinkSchemaString));
        timeFormat = new SimpleDateFormat((String) PowerMock.field(SinkSchemaString.class, "TIME_FORMATTER").get(sinkSchemaString));
        timestampFormat = new SimpleDateFormat((String) PowerMock.field(SinkSchemaString.class, "TIMESTAMP_FORMATTER").get(sinkSchemaString));

        now = new java.util.Date();
        date = dateFormat.parse(dateFormat.format(now));
        time = timeFormat.parse(timeFormat.format(now));
        timestamp = timestampFormat.parse(timestampFormat.format(now));
        timestampWithTimeZoneString = "2011-12-03T10:15:30.030431+01:00";
    }

    @Test
    public void testConvertStructWhenHiveFieldTypeIsMatching() {
        java.sql.Date expectDate = new java.sql.Date(date.getTime() - TimeZone.getDefault().getRawOffset());
        java.sql.Timestamp expectTime = new java.sql.Timestamp(time.getTime() - TimeZone.getDefault().getRawOffset());
        java.sql.Timestamp expectTimestamp = new java.sql.Timestamp(timestamp.getTime() - TimeZone.getDefault().getRawOffset());
        java.sql.Timestamp expectTimestampWithTimeZone = new java.sql.Timestamp(ZonedTimestamp.parseToDate(timestampWithTimeZoneString).getTime());

        Schema tableSchema = Schemas.createSchemaWithAllFieldType();
        SinkRecord sinkRecord = Schemas.createRecordWithAllFieldType(
            date,
            time,
            timestamp,
            timestampWithTimeZoneString
        );

        Struct struct = (Struct) schemaProjector.projectRecord(sinkRecord, null, tableSchema).value();
        List<Object> data = TextUtil.convertStruct(struct);
        Assert.assertEquals(struct.schema().fields().size(), data.size());
        List<Field> fields = struct.schema().fields();
        Map<String, Integer> fieldMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            fieldMap.put(fields.get(i).name(), i);
        }
        verifyValueAndType(data.get(fieldMap.get("int8_schema")), "42", Byte.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("int16_schema")), "42", Short.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("int32_schema")), "42", Integer.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("int64_schema")), "42", Long.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("float32_schema")), "42.5", Float.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("float64_schema")), "42.5", Double.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("boolean_schema")), "false", Boolean.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("string_schema")), "quoteit", String.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("bytes_schema")), "*", String.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("decimal_schema")), "42.42", BigDecimal.class.getTypeName());

        // Date type, convert to sql date
        verifyValueAndType(data.get(fieldMap.get("date_schema")), expectDate.toString(), java.sql.Date.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("time_schema")), expectTime.toString(), java.sql.Timestamp.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("timestamp_schema")), expectTimestamp.toString(), java.sql.Timestamp.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("debezium_timestamp_schema")), expectTimestampWithTimeZone.toString(), java.sql.Timestamp.class.getTypeName());
    }

    @Test
    public void testConvertStructShouldGetDefaultValueForNull() {
        Schema recordSchema = SchemaBuilder.struct().name("record").version(2)
            .field("int8_schema", SinkSchemaTinyint.getInstance().schemaOf("tinyint"))
            .field("int16_schema", SinkSchemaSmallint.getInstance().schemaOf("smallint"))
            .field("int32_schema", SinkSchemaInt.getInstance().schemaOf("int"))
            .build();
        Struct struct = new Struct(recordSchema);
        struct
            .put("int8_schema", (byte) 42)
            .put("int16_schema", (short) 42)
            .put("int32_schema", null);
        List<Object> data = TextUtil.convertStruct(struct);
        List<Field> fields = struct.schema().fields();
        Map<String, Integer> fieldMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            fieldMap.put(fields.get(i).name(), i);
        }
        Assert.assertEquals(struct.schema().fields().size(), data.size());
        verifyValueAndType(data.get(fieldMap.get("int8_schema")), "42", Byte.class.getTypeName());
        verifyValueAndType(data.get(fieldMap.get("int16_schema")), "42", Short.class.getTypeName());
        Assert.assertEquals(TextUtil.NULL_VALUE, data.get(fieldMap.get("int32_schema")));
    }

    private void verifyValueAndType(
        final Object actualValue,
        final String expectValue,
        final String expectValueJavaTypeName) {
        String valueJavaTypeName = actualValue.getClass().getTypeName();
        assertEquals(expectValueJavaTypeName, valueJavaTypeName);
        assertEquals(expectValue, String.valueOf(actualValue));
    }

    // CHECKSTYLE:OFF
    @Test
    public void testBreakingWhitespace() {
        String newStr = CharMatcher.breakingWhitespace().replaceFrom("abc\n\n\r\r\tnabc\u0001a", "");
        Assert.assertEquals("abcnabc\u0001a", newStr);
    }
    // CHECKSTYLE:ON
}
