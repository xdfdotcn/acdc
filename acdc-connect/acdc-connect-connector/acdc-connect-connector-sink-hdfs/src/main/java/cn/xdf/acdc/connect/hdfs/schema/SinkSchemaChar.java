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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class SinkSchemaChar implements SinkSchema {

    private static final SinkSchema INSTANCE = new SinkSchemaChar();

    private final SinkSchemaString sinkSchemaString;

    public SinkSchemaChar() {
        sinkSchemaString = new SinkSchemaString();
    }

    /**
     * Get the singleton instance.
     * @return SinkSchema
     */
    public static SinkSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return "char";
    }

    @Override
    public Schema schemaOf(final String sinkDataTypeName) {
        return SchemaBuilder
            .string()
            .optional()
            .parameter(SinkSchemas.DATA_TYPE_NAME_KEY, sinkDataTypeName)
            .parameter(SinkSchemas.NAME_KEY, name())
            .build();
    }

    @Override
    public boolean isPromotable(final Type schema) {
        return sinkSchemaString.isPromotable(schema);
    }

    @Override
    public boolean isCompatibility(final Schema source) {
        // TODO 是否需要判断字段长度
        return sinkSchemaString.isCompatibility(source);
    }

    @Override
    public Object convertToJavaTypeValue(final Schema source, final Object recordValue) {
        return sinkSchemaString.convertToJavaTypeValue(source, recordValue);
    }

    @Override
    public Object convertToDbTypeValue(final Schema source, final Object recordValue) {
        return sinkSchemaString.convertToDbTypeValue(source, recordValue);
    }

    @Override
    public String sinkDataTypeNameOf(final Schema schema) {
        return name();
    }
}
