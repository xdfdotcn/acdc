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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class SinkSchemaBinary implements SinkSchema {

    private static final SinkSchema INSTANCE = new SinkSchemaBinary();

    private static final Set<Type> TYPES = Sets.immutableEnumSet(Type.BOOLEAN, Type.BYTES);

    /**
     * Get the singleton instance.
     * @return SinkSchema
     */
    public static SinkSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return "binary";
    }

    @Override
    public Schema schemaOf(final String sinkDataTypeName) {
        return SchemaBuilder
            .bytes()
            .optional()
            .parameter(SinkSchemas.DATA_TYPE_NAME_KEY, sinkDataTypeName)
            .parameter(SinkSchemas.NAME_KEY, name())
            .build();
    }

    @Override
    public boolean isPromotable(final Type schema) {
        return TYPES.contains(schema);
    }

    @Override
    public boolean isCompatibility(final Schema source) {
        if (!Strings.isNullOrEmpty(source.name())) {
            return false;
        }
        return isPromotable(source.type());
    }

    @Override
    public Object convertToJavaTypeValue(final Schema schema, final Object recordValue) {
        // TODO 兼容 bit数据类型为1位的情况,connect 传入的数据类型是 boolean
        if (recordValue instanceof Boolean) {
            Boolean boolVal = (Boolean) recordValue;
            Integer intVal = boolVal ? NumberUtils.INTEGER_ONE : NumberUtils.INTEGER_ZERO;
            return String.valueOf(intVal).getBytes(StandardCharsets.UTF_8);
        }
        return recordValue;
    }

    @Override
    public Object convertToDbTypeValue(final Schema schema, final Object recordValue) {
        return convertBytesToBytes(schema, recordValue);
    }

    @Override
    public String sinkDataTypeNameOf(final Schema schema) {
        return name();
    }
}
