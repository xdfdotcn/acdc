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

package cn.xdf.acdc.connect.hdfs.common;

import cn.xdf.acdc.connect.core.sink.data.ZonedTimestamp;
import cn.xdf.acdc.connect.hdfs.schema.SinKSchemaDouble;
import cn.xdf.acdc.connect.hdfs.schema.SinkSchema;
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
import cn.xdf.acdc.connect.hdfs.schema.SinkSchemas;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;

public class Schemas {

    /**
     Create sink schema with all data type.
     @return sink table schema
     */
    public static Schema createSchemaWithAllFieldType() {
        Schema tableSchema = SchemaBuilder.struct().name("record").version(1)
            .field("int8_schema", SinkSchemaTinyint.getInstance().schemaOf("tinyint"))
            .field("int16_schema", SinkSchemaSmallint.getInstance().schemaOf("smallint"))
            .field("int32_schema", SinkSchemaInt.getInstance().schemaOf("int"))
            .field("int64_schema", SinkSchemaBigint.getInstance().schemaOf("bigint"))
            .field("float32_schema", SinkSchemaFloat.getInstance().schemaOf("float"))
            .field("float64_schema", SinKSchemaDouble.getInstance().schemaOf("double"))
            .field("boolean_schema", SinkSchemaBoolean.getInstance().schemaOf("boolean"))
            .field("string_schema", SinkSchemaString.getInstance().schemaOf("string"))
            .field("bytes_schema", SinkSchemaBinary.getInstance().schemaOf("binary"))
            .field("decimal_schema", SinkSchemaDecimal.getInstance().schemaOf("decimal(38,2)"))
            .field("date_schema", SinkSchemaDate.getInstance().schemaOf("date"))
            .field("time_schema", SinkSchemaTimestamp.getInstance().schemaOf("timestamp"))
            .field("timestamp_schema", SinkSchemaTimestamp.getInstance().schemaOf("timestamp"))
            .field("debezium_timestamp_schema", SinkSchemaTimestamp.getInstance().schemaOf("timestamp"))
            .build();
        return tableSchema;
    }

    /**
     * Create sink record with all data type.
     * @param date date
     * @param time  time
     * @param timestamp timestamp
     * @param timestampWithTimeZoneString  timestamp with zone
     * @return sink record
     */
    public static SinkRecord createRecordWithAllFieldType(
        final java.util.Date date,
        final java.util.Date time,
        final java.util.Date timestamp,
        final String timestampWithTimeZoneString
    ) {
        Schema recordSchema = SchemaBuilder.struct().name("record").version(2)
            .field("int8_schema", Schema.INT8_SCHEMA)
            .field("int16_schema", Schema.INT16_SCHEMA)
            .field("int32_schema", Schema.INT32_SCHEMA)
            .field("int64_schema", Schema.INT64_SCHEMA)
            .field("float32_schema", Schema.FLOAT32_SCHEMA)
            .field("float64_schema", Schema.FLOAT64_SCHEMA)
            .field("boolean_schema", Schema.BOOLEAN_SCHEMA)
            .field("string_schema", Schema.STRING_SCHEMA)
            .field("bytes_schema", Schema.BYTES_SCHEMA)
            .field("decimal_schema", Decimal.builder(2))
            .field("date_schema", Date.SCHEMA)
            .field("time_schema", Time.SCHEMA)
            .field("timestamp_schema", Timestamp.SCHEMA)
            .field("debezium_timestamp_schema", SchemaBuilder.string().name(ZonedTimestamp.LOGICAL_NAME).build())
            .build();
        Struct struct = new Struct(recordSchema);
        struct
            .put("int8_schema", (byte) 42)
            .put("int16_schema", (short) 42)
            .put("int32_schema", 42)
            .put("int64_schema", 42L)
            .put("float32_schema", 42.5f)
            .put("float64_schema", 42.5d)
            .put("boolean_schema", false)
            .put("string_schema", "quoteit")
            .put("bytes_schema", new byte[] {42})
            .put("decimal_schema", new BigDecimal("42.42"))
            .put("date_schema", date)
            .put("time_schema", time)
            .put("timestamp_schema", timestamp)
            .put("debezium_timestamp_schema", timestampWithTimeZoneString);
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

    /**
     * Create sink record with all string type.
     * @return sink table schema
     */
    public static Schema createSchemaWithStringFieldType() {
        Schema tableSchema = SchemaBuilder.struct().name("record").version(1)
            .field("int8_schema", Schema.STRING_SCHEMA)
            .field("int16_schema", Schema.STRING_SCHEMA)
            .field("int32_schema", Schema.STRING_SCHEMA)
            .field("int64_schema", Schema.STRING_SCHEMA)
            .field("float32_schema", Schema.STRING_SCHEMA)
            .field("float64_schema", Schema.STRING_SCHEMA)
            .field("boolean_schema", Schema.STRING_SCHEMA)
            .field("string_schema", Schema.STRING_SCHEMA)
            .field("bytes_schema", Schema.STRING_SCHEMA)
            .field("decimal_schema", Schema.STRING_SCHEMA)
            .field("date_schema", Schema.STRING_SCHEMA)
            .field("time_schema", Schema.STRING_SCHEMA)
            .field("timestamp_schema", Schema.STRING_SCHEMA)
            .field("debezium_timestamp_schema", Schema.STRING_SCHEMA)
            .build();
        return tableSchema;
    }

    /**
     * Create hive table schema.
     * @return field schema list
     */
    public static List<FieldSchema> createHivePrimitiveSchemaWithAllFieldType() {
        List<FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(new FieldSchema("tinyint_schema", "tinyint", null));
        fieldSchemaList.add(new FieldSchema("smallint_schema", "smallint", null));
        fieldSchemaList.add(new FieldSchema("int_schema", "int", null));
        fieldSchemaList.add(new FieldSchema("bigint_schema", "bigint", null));
        fieldSchemaList.add(new FieldSchema("float_schema", "float", null));
        fieldSchemaList.add(new FieldSchema("double_schema", "double", null));
        fieldSchemaList.add(new FieldSchema("boolean_schema", "boolean", null));
        fieldSchemaList.add(new FieldSchema("string_schema", "string", null));
        fieldSchemaList.add(new FieldSchema("varchar_schema", "varchar(50)", null));
        fieldSchemaList.add(new FieldSchema("char_schema", "char(20)", null));
        fieldSchemaList.add(new FieldSchema("binary_schema", "binary", null));
        fieldSchemaList.add(new FieldSchema("decimal_schema", "decimal(38,7)", null));
        fieldSchemaList.add(new FieldSchema("date_schema", "date", null));
        fieldSchemaList.add(new FieldSchema("timestamp_schema", "timestamp", null));
        /**
         42,42,42,42,42.5,42.5,false,quoteit,varchar_test,char_test,*,42.42,2021-08-10,2021-08-10 02:14:18.0
         */
        return fieldSchemaList;
    }

    /**
     *  Create hive primitive table record.
     *  @param date  date
     *  @param timestamp  timestamp
     *  @return record
     */
    public static SinkRecord createHivePrimitiveRecordWithAllFieldType(
        final java.util.Date date,
        final java.util.Date timestamp
    ) {
        Schema recordSchema = SchemaBuilder.struct().name("record").version(2)
            .field("tinyint_schema", Schema.INT8_SCHEMA)
            .field("smallint_schema", Schema.INT16_SCHEMA)
            .field("int_schema", Schema.INT32_SCHEMA)
            .field("bigint_schema", Schema.INT64_SCHEMA)
            .field("float_schema", Schema.FLOAT32_SCHEMA)
            .field("double_schema", Schema.FLOAT64_SCHEMA)
            .field("boolean_schema", Schema.BOOLEAN_SCHEMA)
            .field("string_schema", Schema.STRING_SCHEMA)
            .field("varchar_schema", Schema.STRING_SCHEMA)
            .field("char_schema", Schema.STRING_SCHEMA)
            .field("binary_schema", Schema.BYTES_SCHEMA)
            .field("decimal_schema", Decimal.builder(2))
            .field("date_schema", Date.SCHEMA)
            .field("timestamp_schema", Timestamp.SCHEMA)
            .build();
        Struct struct = new Struct(recordSchema);
        struct
            .put("tinyint_schema", (byte) 42)
            .put("smallint_schema", (short) 42)
            .put("int_schema", 42)
            .put("bigint_schema", 42L)
            .put("float_schema", 42.5f)
            .put("double_schema", 42.5d)
            .put("boolean_schema", false)
            .put("string_schema", "quoteit")
            .put("varchar_schema", "varchar_test")
            .put("char_schema", "char_test")
            .put("binary_schema", new byte[] {42})
            .put("decimal_schema", new BigDecimal("42.42"))
            .put("date_schema", date)
            .put("timestamp_schema", timestamp);
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

    private static Schema convert(final Schema schema) {
        SinkSchema sinkSchema = SinkSchemas.sinkSchemaOf(schema);
        String sinkDataTypeName = sinkSchema.sinkDataTypeNameOf(schema);
        return sinkSchema.schemaOf(sinkDataTypeName);
    }
}
