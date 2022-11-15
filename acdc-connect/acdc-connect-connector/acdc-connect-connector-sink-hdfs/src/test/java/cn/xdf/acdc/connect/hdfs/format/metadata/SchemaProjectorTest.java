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

package cn.xdf.acdc.connect.hdfs.format.metadata;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.hdfs.HdfsSinkConstants;
import cn.xdf.acdc.connect.hdfs.common.Schemas;
import cn.xdf.acdc.connect.hdfs.schema.SinKSchemaDouble;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaBigint;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaBinary;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaBoolean;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaDate;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaDecimal;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaFloat;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaInt;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaSmallint;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaString;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaTimestamp;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemaTinyint;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 {@link SchemaProjector}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SinkSchemaString.class)
@PowerMockIgnore("javax.management.*")

public class SchemaProjectorTest {

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
    public void testProjectRecordWhenFieldTypeIsMatching() throws UnsupportedEncodingException {
        java.util.Date expectDate = new java.util.Date(date.getTime() - TimeZone.getDefault().getRawOffset());
        java.util.Date expectTime = new java.util.Date(time.getTime() - TimeZone.getDefault().getRawOffset());
        java.util.Date expectTimestamp = new java.util.Date(timestamp.getTime() - TimeZone.getDefault().getRawOffset());
        java.util.Date expectTimestampWithTimeZone = ZonedTimestamp.parseToDate(timestampWithTimeZoneString);

        // 2011-12-03T10:15:30.030431+01:00
        Schema tableSchema = Schemas.createSchemaWithAllFieldType();
        SinkRecord sinkRecord = Schemas.createRecordWithAllFieldType(date, time, timestamp, timestampWithTimeZoneString);
        SinkRecord projectedSinkRecord = schemaProjector.projectRecord(sinkRecord, null, tableSchema);
        Struct projectedSinkRecordStruct = (Struct) projectedSinkRecord.value();

        assertEquals(tableSchema, projectedSinkRecord.valueSchema());
        verifyValueAndType("int8_schema", projectedSinkRecordStruct, "42", Byte.class.getTypeName());
        verifyValueAndType("int16_schema", projectedSinkRecordStruct, "42", Short.class.getTypeName());
        verifyValueAndType("int32_schema", projectedSinkRecordStruct, "42", Integer.class.getTypeName());
        verifyValueAndType("int64_schema", projectedSinkRecordStruct, "42", Long.class.getTypeName());
        verifyValueAndType("float32_schema", projectedSinkRecordStruct, "42.5", Float.class.getTypeName());
        verifyValueAndType("float64_schema", projectedSinkRecordStruct, "42.5", Double.class.getTypeName());
        verifyValueAndType("boolean_schema", projectedSinkRecordStruct, "false", Boolean.class.getTypeName());
        verifyValueAndType("string_schema", projectedSinkRecordStruct, "quoteit", String.class.getTypeName());
        verifyValueAndType("bytes_schema", projectedSinkRecordStruct, "*", "byte[]");
        verifyValueAndType("decimal_schema", projectedSinkRecordStruct, "42.42", BigDecimal.class.getTypeName());
        verifyValueAndType("date_schema", projectedSinkRecordStruct, expectDate.toString(), java.util.Date.class.getTypeName());
        verifyValueAndType("time_schema", projectedSinkRecordStruct, expectTime.toString(), java.util.Date.class.getTypeName());
        verifyValueAndType("timestamp_schema", projectedSinkRecordStruct, expectTimestamp.toString(), java.util.Date.class.getTypeName());
        verifyValueAndType("debezium_timestamp_schema", projectedSinkRecordStruct, expectTimestampWithTimeZone.toString(), java.util.Date.class.getTypeName());
    }

    @Test
    public void testProjectRecordWhenHiveFieldTypeIsString() throws UnsupportedEncodingException {
        String expectDate = dateFormat.format(new java.util.Date(date.getTime() - TimeZone.getDefault().getRawOffset()));
        String expectTime = timeFormat.format(new java.util.Date(time.getTime() - TimeZone.getDefault().getRawOffset()));
        String expectTimestamp = timestampFormat.format(new java.util.Date(timestamp.getTime() - TimeZone.getDefault().getRawOffset()));
        String expectTimestampWithTimeZone = timestampFormat.format(ZonedTimestamp.parseToDate(timestampWithTimeZoneString));

        // 2011-12-03T10:15:30.030431+01:00
        Schema tableSchema = Schemas.createSchemaWithStringFieldType();
        SinkRecord sinkRecord = Schemas.createRecordWithAllFieldType(date, time, timestamp, timestampWithTimeZoneString);
        SinkRecord projectedSinkRecord = schemaProjector.projectRecord(sinkRecord, null, tableSchema);
        Struct projectedSinkRecordStruct = (Struct) projectedSinkRecord.value();

        assertEquals(tableSchema, projectedSinkRecord.valueSchema());
        verifyValueAndType("int8_schema", projectedSinkRecordStruct, "42", String.class.getTypeName());
        verifyValueAndType("int16_schema", projectedSinkRecordStruct, "42", String.class.getTypeName());
        verifyValueAndType("int32_schema", projectedSinkRecordStruct, "42", String.class.getTypeName());
        verifyValueAndType("int64_schema", projectedSinkRecordStruct, "42", String.class.getTypeName());
        verifyValueAndType("float32_schema", projectedSinkRecordStruct, "42.5", String.class.getTypeName());
        verifyValueAndType("float64_schema", projectedSinkRecordStruct, "42.5", String.class.getTypeName());
        verifyValueAndType("boolean_schema", projectedSinkRecordStruct, "false", String.class.getTypeName());
        verifyValueAndType("string_schema", projectedSinkRecordStruct, "quoteit", String.class.getTypeName());
        verifyValueAndType("bytes_schema", projectedSinkRecordStruct, "*", String.class.getTypeName());
        verifyValueAndType("decimal_schema", projectedSinkRecordStruct, "42.42", String.class.getTypeName());
        verifyValueAndType("date_schema", projectedSinkRecordStruct, expectDate, String.class.getTypeName());
        verifyValueAndType("time_schema", projectedSinkRecordStruct, expectTime, String.class.getTypeName());
        verifyValueAndType("timestamp_schema", projectedSinkRecordStruct, expectTimestamp, String.class.getTypeName());
        verifyValueAndType("debezium_timestamp_schema", projectedSinkRecordStruct, expectTimestampWithTimeZone, String.class.getTypeName());
    }

    @Test
    public void testProjectRecordShouldNotCompatibilityWhenDifferentFieldType() {
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaTinyint.getInstance().schemaOf("tinyint")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaSmallint.getInstance().schemaOf("smallint")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaInt.getInstance().schemaOf("int")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaBigint.getInstance().schemaOf("bigint")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaFloat.getInstance().schemaOf("float")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinKSchemaDouble.getInstance().schemaOf("double")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaBoolean.getInstance().schemaOf("boolean")));
//        assertTrue(!schemaProjector.isPromotable(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaBinary.getInstance().schemaOf("binary")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaDecimal.getInstance().schemaOf("decimal(38,2)")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaDate.getInstance().schemaOf("date")));
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaTimestamp.getInstance().schemaOf("timestamp")));

        // date not promotable
        assertTrue(!schemaProjector.isCompatibility(Date.SCHEMA, SinkSchemaTimestamp.getInstance().schemaOf("timestamp")));
        assertTrue(!schemaProjector.isCompatibility(Timestamp.SCHEMA, SinkSchemaDate.getInstance().schemaOf("date")));
        assertTrue(!schemaProjector.isCompatibility(Time.SCHEMA, SinkSchemaDate.getInstance().schemaOf("date")));
    }

    @Test
    public void testProjectRecordShouldThrownExceptionWithNotSupportTimeFieldType() {
        assertTrue(!schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaTimestamp.getInstance().schemaOf("timestamp")));
    }

    @Test
    public void testProjectRecordShouldCompatibilityWhenSpecificFieldType() {
        // Target is String
        assertTrue(schemaProjector.isCompatibility(Schema.INT8_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT16_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT32_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT64_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.FLOAT32_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.FLOAT64_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.BOOLEAN_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Schema.BYTES_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Decimal.builder(2), SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Date.SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Time.SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));
        assertTrue(schemaProjector.isCompatibility(Timestamp.SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));

        // Type mapping
        assertTrue(schemaProjector.isCompatibility(Schema.INT8_SCHEMA, SinkSchemaTinyint.getInstance().schemaOf("tinyint")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT16_SCHEMA, SinkSchemaTinyint.getInstance().schemaOf("tinyint")));

        assertTrue(schemaProjector.isCompatibility(Schema.INT16_SCHEMA, SinkSchemaSmallint.getInstance().schemaOf("smallint")));

        assertTrue(schemaProjector.isCompatibility(Schema.INT32_SCHEMA, SinkSchemaInt.getInstance().schemaOf("int")));

        assertTrue(schemaProjector.isCompatibility(Schema.INT64_SCHEMA, SinkSchemaBigint.getInstance().schemaOf("bigint")));

        assertTrue(schemaProjector.isCompatibility(Schema.FLOAT32_SCHEMA, SinkSchemaFloat.getInstance().schemaOf("float")));
        assertTrue(schemaProjector.isCompatibility(Schema.FLOAT64_SCHEMA, SinkSchemaFloat.getInstance().schemaOf("float")));

        assertTrue(schemaProjector.isCompatibility(Schema.FLOAT64_SCHEMA, SinKSchemaDouble.getInstance().schemaOf("double")));

        assertTrue(schemaProjector.isCompatibility(Schema.BOOLEAN_SCHEMA, SinkSchemaBoolean.getInstance().schemaOf("boolean")));

        assertTrue(schemaProjector.isCompatibility(Schema.STRING_SCHEMA, SinkSchemaString.getInstance().schemaOf("string")));

        assertTrue(schemaProjector.isCompatibility(Schema.BYTES_SCHEMA, SinkSchemaBinary.getInstance().schemaOf("binary")));
        assertTrue(schemaProjector.isCompatibility(Schema.BOOLEAN_SCHEMA, SinkSchemaBinary.getInstance().schemaOf("binary")));

        assertTrue(schemaProjector.isCompatibility(Decimal.builder(2), SinkSchemaDecimal.getInstance().schemaOf("decimal(38,2)")));

        assertTrue(schemaProjector.isCompatibility(Date.SCHEMA, SinkSchemaDate.getInstance().schemaOf("date")));

        assertTrue(schemaProjector.isCompatibility(Timestamp.SCHEMA, SinkSchemaTimestamp.getInstance().schemaOf("timestamp")));

        // Date  promotable
        assertTrue(schemaProjector.isCompatibility(SchemaBuilder.string().name(ZonedTimestamp.LOGICAL_NAME).build(), Timestamp.SCHEMA));
        assertTrue(schemaProjector.isCompatibility(Time.SCHEMA, SinkSchemaTimestamp.getInstance().schemaOf("timestamp")));
        assertTrue(schemaProjector.isCompatibility(Date.SCHEMA, SinkSchemaDate.getInstance().schemaOf("date")));

        //Backwards compatible
        assertTrue(schemaProjector.isCompatibility(Schema.INT8_SCHEMA, SinkSchemaSmallint.getInstance().schemaOf("smallint")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT16_SCHEMA, SinkSchemaInt.getInstance().schemaOf("int")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT32_SCHEMA, SinkSchemaBigint.getInstance().schemaOf("bigint")));
        assertTrue(schemaProjector.isCompatibility(Schema.INT64_SCHEMA, SinkSchemaFloat.getInstance().schemaOf("float")));
        assertTrue(schemaProjector.isCompatibility(Schema.FLOAT32_SCHEMA, SinkSchemaFloat.getInstance().schemaOf("float")));
    }

    @Test(expected = ConnectException.class)
    public void testCheckCompatibilityThrownExceptionWithNotCompatibility() {
        Schema targetSchema = SchemaBuilder.struct().name("record").version(1)
            .field("date_schema", SinkSchemaDate.getInstance().schemaOf("date"))
            .field("time_schema", SinkSchemaTimestamp.getInstance().schemaOf("timestamp"))
            .field("timestamp_schema", SinkSchemaTimestamp.getInstance().schemaOf("timestamp"))
            .build();

        Schema sourceSchema = SchemaBuilder.struct().name("record").version(1)
            .field("date_schema", Schema.STRING_SCHEMA)
            .field("time_schema", Schema.STRING_SCHEMA)
            .field("timestamp_schema", Schema.STRING_SCHEMA)
            .build();
        schemaProjector.checkCompatibility(sourceSchema, targetSchema);
    }

    private void verifyValueAndType(
        final String fieldName,
        final Struct struct,
        final String expectValue,
        final String javaTypeName) throws UnsupportedEncodingException {
        Object value = struct.get(fieldName);
        String valueJavaTypeName = value.getClass().getTypeName();
        System.out.println("name: " + fieldName + " value type :" + value.getClass());
        assertEquals(javaTypeName, value.getClass().getTypeName());
        if ("byte[]".equals(valueJavaTypeName)) {
            value = new String(new byte[] {42}, HdfsSinkConstants.UTF8_CHARACTER);
        }
        assertEquals(expectValue, String.valueOf(value));
    }
}
