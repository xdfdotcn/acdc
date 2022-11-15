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

package cn.xdf.acdc.connect.hdfs.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

public class SchemaConverter {

    private static final Map<Type, TypeInfo> TYPE_TO_TYPEINFO;

    /**
     * the name has to be consistent with {@link io.confluent.connect.avro.AvroData }, when Connect Decimal
     * schema is created, this property name is used to set precision.
     * We have to use the exact name to retrieve precision value.
     */
    private static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

    // this is the maximum digits Hive allows for DECIMAL type.
    private static final int HIVE_DECIMAL_PRECISION_MAX = 38;

    static {
        TYPE_TO_TYPEINFO = new HashMap<>();
        TYPE_TO_TYPEINFO.put(Type.BOOLEAN, TypeInfoFactory.booleanTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.INT8, TypeInfoFactory.byteTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.INT16, TypeInfoFactory.shortTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.INT32, TypeInfoFactory.intTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.INT64, TypeInfoFactory.longTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.FLOAT32, TypeInfoFactory.floatTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.FLOAT64, TypeInfoFactory.doubleTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.BYTES, TypeInfoFactory.binaryTypeInfo);
        TYPE_TO_TYPEINFO.put(Type.STRING, TypeInfoFactory.stringTypeInfo);
    }

    /**
     * Convert kafka record value schema to {@link org.apache.hadoop.hive.metastore.api.FieldSchema }.
     * @param schema  kafka record value schema
     * @return filed schema list
     */
    public static List<FieldSchema> convertSchema(final Schema schema) {
        List<FieldSchema> columns = new ArrayList<>();
        if (Type.STRUCT.equals(schema.type())) {
            for (Field field : schema.fields()) {
                columns.add(new FieldSchema(
                    field.name(), convert(field.schema()).getTypeName(), field.schema().doc()));
            }
        }
        return columns;
    }

    /**
     * Convert kafka record value schema to {@link org.apache.hadoop.hive.metastore.api.FieldSchema }.
     * @param schema  kafka record value schema
     * @return filed schema list
     */
    public static List<FieldSchema> convertSchemaMaybeLogical(final Schema schema) {
        List<FieldSchema> columns = new ArrayList<>();
        if (Type.STRUCT.equals(schema.type())) {
            for (Field field : schema.fields()) {
                columns.add(new FieldSchema(
                    field.name(), convertMaybeLogical(field.schema()).getTypeName(), field.schema().doc()));
            }
        }
        return columns;
    }

    /**
     * Convert kafka record value schema to {@link org.apache.hadoop.hive.serde2.typeinfo.TypeInfo }.
     * @param schema  kafka record value schema
     * @return TypeInfo
     */
    public static TypeInfo convert(final Schema schema) {
        // TODO: throw an error on recursive types
        switch (schema.type()) {
            case STRUCT:
                return convertStruct(schema);
            case ARRAY:
                return convertArray(schema);
            case MAP:
                return convertMap(schema);
            default:
                return convertPrimitive(schema.type());
        }
    }

    private static TypeInfo convertMaybeLogical(final Schema schema) {
        switch (schema.type()) {
            case STRUCT:
                return convertStruct(schema);
            case ARRAY:
                return convertArray(schema);
            case MAP:
                return convertMap(schema);
            default:
                return convertPrimitiveMaybeLogical(schema);
        }
    }

    private static TypeInfo convertStruct(final Schema schema) {
        final List<Field> fields = schema.fields();
        final List<String> names = new ArrayList<>(fields.size());
        final List<TypeInfo> types = new ArrayList<>(fields.size());
        for (Field field : fields) {
            names.add(field.name());
            types.add(convert(field.schema()));
        }
        return TypeInfoFactory.getStructTypeInfo(names, types);
    }

    private static TypeInfo convertArray(final Schema schema) {
        return TypeInfoFactory.getListTypeInfo(convert(schema.valueSchema()));
    }

    private static TypeInfo convertMap(final Schema schema) {
        return TypeInfoFactory.getMapTypeInfo(
            convert(schema.keySchema()), convert(schema.valueSchema()));
    }

    private static TypeInfo convertPrimitive(final Type schemaType) {
        return TYPE_TO_TYPEINFO.get(schemaType);
    }

    private static TypeInfo convertPrimitiveMaybeLogical(final Schema schema) {
        if (schema.name() == null) {
            return convertPrimitive(schema.type());
        }

        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            String scale = schema.parameters().get(Decimal.SCALE_FIELD);
            String precision = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
            if (precision != null && Integer.parseInt(precision) > HIVE_DECIMAL_PRECISION_MAX) {
                throw new ConnectException(
                    String.format("Illegal precision %s : Hive allows at most %d precision.",
                        precision,
                        HIVE_DECIMAL_PRECISION_MAX)
                );
            }
            // Let precision always be HIVE_DECIMAL_PRECISION_MAX. Hive serde will try the best
            // to fit decimal data into decimal schema. If the data is too long even for
            // the maximum precision, hive will throw serde exception. No data loss risk.
            return new DecimalTypeInfo(HIVE_DECIMAL_PRECISION_MAX, Integer.parseInt(scale));
        }
        if (Date.LOGICAL_NAME.equals(schema.name())) {
            return TypeInfoFactory.dateTypeInfo;
        }

        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TypeInfoFactory.timestampTypeInfo;
        }

        if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TypeInfoFactory.timestampTypeInfo;
        }
        return convertPrimitive(schema.type());
    }

}
