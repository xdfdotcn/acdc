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

package cn.xdf.acdc.connect.hdfs.format.orc;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public final class OrcUtil {

    private static final Map<Type, BiFunction<Struct, Field, Object>> CONVERSION_MAP = new HashMap<>();

    static {
        CONVERSION_MAP.put(Schema.Type.ARRAY, OrcUtil::convertArray);
        CONVERSION_MAP.put(Schema.Type.BOOLEAN, OrcUtil::convertBoolean);
        CONVERSION_MAP.put(Schema.Type.BYTES, OrcUtil::convertBytes);
        CONVERSION_MAP.put(Schema.Type.FLOAT32, OrcUtil::convertFloat32);
        CONVERSION_MAP.put(Schema.Type.FLOAT64, OrcUtil::convertFloat64);
        CONVERSION_MAP.put(Schema.Type.INT8, OrcUtil::convertInt8);
        CONVERSION_MAP.put(Schema.Type.INT16, OrcUtil::convertInt16);
        CONVERSION_MAP.put(Schema.Type.INT32, OrcUtil::convertInt32);
        CONVERSION_MAP.put(Schema.Type.INT64, OrcUtil::convertInt64);
        CONVERSION_MAP.put(Schema.Type.MAP, OrcUtil::convertMap);
        CONVERSION_MAP.put(Schema.Type.STRING, OrcUtil::convertString);
        CONVERSION_MAP.put(Schema.Type.STRUCT, OrcUtil::convertStruct);
    }

    /**
     * Create an object of OrcStruct given a type and a list of objects.
     *
     * @param typeInfo the type info
     * @param objs the objects corresponding to the struct fields
     * @return the struct object
     */
    @SuppressWarnings("unchecked")
    public static OrcStruct createOrcStruct(final TypeInfo typeInfo, final Object... objs) {
        SettableStructObjectInspector oi = (SettableStructObjectInspector)
            OrcStruct.createObjectInspector(typeInfo);

        List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
        OrcStruct result = (OrcStruct) oi.create();
        result.setNumFields(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            oi.setStructFieldData(result, fields.get(i), objs[i]);
        }

        return result;
    }

    /**
     * Convert a Struct into a Writable array.
     *
     * @param struct the struct to convert
     * @return the struct as a writable array
     */
    public static Object[] convertStruct(final Struct struct) {
        List<Object> data = new LinkedList<>();
        for (Field field : struct.schema().fields()) {
            if (struct.get(field) == null) {
                data.add(null);
            } else {
                Type schemaType = field.schema().type();
                data.add(CONVERSION_MAP.get(schemaType).apply(struct, field));
            }
        }

        return data.toArray();
    }

    private static Object convertStruct(final Struct struct, final Field field) {
        return convertStruct(struct.getStruct(field.name()));
    }

    private static Object convertArray(final Struct struct, final Field field) {
        return new ArrayPrimitiveWritable(struct.getArray(field.name()).toArray());
    }

    private static Object convertBoolean(final Struct struct, final Field field) {
        return new BooleanWritable(struct.getBoolean(field.name()));
    }

    private static Object convertBytes(final Struct struct, final Field field) {
        return new BytesWritable(struct.getBytes(field.name()));
    }

    private static Object convertFloat32(final Struct struct, final Field field) {
        return new FloatWritable(struct.getFloat32(field.name()));
    }

    private static Object convertFloat64(final Struct struct, final Field field) {
        return new DoubleWritable(struct.getFloat64(field.name()));
    }

    private static Object convertInt8(final Struct struct, final Field field) {
        return new ByteWritable(struct.getInt8(field.name()));
    }

    private static Object convertInt16(final Struct struct, final Field field) {
        return new ShortWritable(struct.getInt16(field.name()));
    }

    private static Object convertInt32(final Struct struct, final Field field) {

        if (Date.LOGICAL_NAME.equals(field.schema().name())) {
            java.util.Date date = (java.util.Date) struct.get(field);
            return new DateWritable(new java.sql.Date(date.getTime()));
        }

        if (Time.LOGICAL_NAME.equals(field.schema().name())) {
            java.util.Date date = (java.util.Date) struct.get(field);
            return new TimestampWritable(new java.sql.Timestamp(date.getTime()));
        }

        return new IntWritable(struct.getInt32(field.name()));
    }

    private static Object convertInt64(final Struct struct, final Field field) {

        if (Timestamp.LOGICAL_NAME.equals(field.schema().name())) {
            java.util.Date date = (java.util.Date) struct.get(field);
            return new TimestampWritable(new java.sql.Timestamp(date.getTime()));
        }

        return new LongWritable(struct.getInt64(field.name()));
    }

    private static Object convertMap(final Struct struct, final Field field) {
        MapWritable mapWritable = new MapWritable();
        struct.getMap(field.name()).forEach(
            (key, value) -> mapWritable.put(new ObjectWritable(key), new ObjectWritable(value))
        );

        return mapWritable;
    }

    private static Object convertString(final Struct struct, final Field field) {
        return new Text(struct.getString(field.name()));
    }
}
