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

package cn.xdf.acdc.connect.hdfs.schema;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public class SinkSchemas {

    public static final String DATA_TYPE_NAME_KEY = "_data_type_name";

    public static final String NAME_KEY = "_name";

    private static final Pattern TYPE_NAME_PATTERN = Pattern.compile("(^[a-zA-Z]+)(.)*");

    private static final Map<String, SinkSchema> TYPE_MAP = Lists.newArrayList(
        SinkSchemaTinyint.getInstance(),
        SinkSchemaSmallint.getInstance(),
        SinkSchemaInt.getInstance(),
        SinkSchemaBigint.getInstance(),
        SinkSchemaFloat.getInstance(),
        SinKSchemaDouble.getInstance(),
        SinkSchemaBoolean.getInstance(),
        SinkSchemaString.getInstance(),
        SinkSchemaVarchar.getInstance(),
        SinkSchemaChar.getInstance(),
        SinkSchemaBinary.getInstance(),
        SinkSchemaDate.getInstance(),
        SinkSchemaTimestamp.getInstance(),
        SinkSchemaDecimal.getInstance()
    ).stream().collect(Collectors.toMap(SinkSchema::name, sinkSchema -> sinkSchema));

    static {
        // logical  type
        TYPE_MAP.put("org.apache.kafka.connect.data.Date", SinkSchemaString.getInstance());
        TYPE_MAP.put("org.apache.kafka.connect.data.Time", SinkSchemaString.getInstance());
        TYPE_MAP.put("org.apache.kafka.connect.data.Timestamp", SinkSchemaString.getInstance());
        TYPE_MAP.put("io.debezium.time.ZonedTimestamp", SinkSchemaString.getInstance());
        TYPE_MAP.put("org.apache.kafka.connect.data.Decimal", SinkSchemaString.getInstance());
        TYPE_MAP.put("io.debezium.data.Bits", SinkSchemaString.getInstance());
        TYPE_MAP.put("io.debezium.data.Json", SinkSchemaString.getInstance());

        // primitive type
        TYPE_MAP.put("INT8", SinkSchemaString.getInstance());
        TYPE_MAP.put("INT16", SinkSchemaString.getInstance());
        TYPE_MAP.put("INT32", SinkSchemaString.getInstance());
        TYPE_MAP.put("INT64", SinkSchemaString.getInstance());
        TYPE_MAP.put("FLOAT32", SinkSchemaString.getInstance());
        TYPE_MAP.put("FLOAT64", SinkSchemaString.getInstance());
        TYPE_MAP.put("BOOLEAN", SinkSchemaString.getInstance());
        TYPE_MAP.put("STRING", SinkSchemaString.getInstance());
        TYPE_MAP.put("BYTES", SinkSchemaString.getInstance());
    }

    /**
     * Match sink schema.
     * @param schema record schema
     * @return sink schema
     */
    public static SinkSchema sinkSchemaOf(final Schema schema) {
        return sinkSchemaOf(nameOf(schema));
    }

    private static SinkSchema sinkSchemaOf(final String name) {
        SinkSchema sinkSchema = TYPE_MAP.get(name);
        Preconditions.checkNotNull(sinkSchema, "No matched sink schema for: " + name);
        return sinkSchema;
    }

    /**
     * Convert record struct schema to hive schema.
     * @param structSchema record schema
     * @return hive field schema
     */
    public static List<FieldSchema> convertToFieldColumns(final Schema structSchema) {
        Preconditions.checkArgument(Type.STRUCT.equals(structSchema.type()));
        List<FieldSchema> columns = new ArrayList<>();
        for (Field field : structSchema.fields()) {
            Schema fieldSchema = field.schema();
            String fieldName = field.name();
            String doc = field.schema().doc();
            String sinkDataTypeName = sinkDataTypeNameOf(fieldSchema);
            columns.add(new FieldSchema(fieldName, sinkDataTypeName, doc));
        }
        return columns;
    }

    /**
     * Convert hive field schema to record schema.
     * @param structFieldList hive table filed schema list
     * @param schemaBuilder  record schema builder
     * @return hive field schema
     */
    public static Schema convertToStructSchema(final List<StructField> structFieldList, final SchemaBuilder schemaBuilder) {
        for (StructField filed : structFieldList) {
            ObjectInspector fieldObjInspector = filed.getFieldObjectInspector();
            Preconditions.checkArgument(Category.PRIMITIVE == fieldObjInspector.getCategory());

            String sinkDataTypeName = filed.getFieldObjectInspector().getTypeName();
            String shortName = shortNameOf(sinkDataTypeName);
            schemaBuilder.field(filed.getFieldName(), sinkSchemaOf(shortName).schemaOf(sinkDataTypeName));
        }
        return schemaBuilder.build();
    }

    private static String nameOf(final Schema schema) {
        if (containKey(schema, NAME_KEY)) {
            return schema.parameters().get(NAME_KEY);
        }
        return Strings.isNullOrEmpty(schema.name()) ? schema.type().name() : schema.name();
    }

    private static String shortNameOf(final String sinkDataTypeName) {
        Matcher m = TYPE_NAME_PATTERN.matcher(sinkDataTypeName);
        if (!m.matches()) {
            throw new ConfigException("Unknown type name");
        }
        return m.group(1);
    }

    private static String sinkDataTypeNameOf(final Schema schema) {
        if (containKey(schema, DATA_TYPE_NAME_KEY)) {
            return schema.parameters().get(DATA_TYPE_NAME_KEY);
        }
        return sinkSchemaOf(nameOf(schema)).sinkDataTypeNameOf(schema);
    }

    private static boolean containKey(final Schema schema, final String key) {
        Map<String, String> params = schema.parameters();
        boolean isEmpty = Objects.isNull(params) || params.isEmpty();
        return !isEmpty && params.containsKey(key);
    }

    /**
     * Whether contain schema name key in the schema's parameters map.
     * @param schema record schema
     * @return whether contain name key
     */
    public static boolean containNameKey(final Schema schema) {
        return containKey(schema, NAME_KEY);
    }
}
