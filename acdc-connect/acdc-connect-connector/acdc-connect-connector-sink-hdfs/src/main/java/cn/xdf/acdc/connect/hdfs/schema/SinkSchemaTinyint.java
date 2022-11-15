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
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

public class SinkSchemaTinyint implements SinkSchema {

    private static final SinkSchema INSTANCE = new SinkSchemaTinyint();

    private static final Set<Type> TYPES = Sets.immutableEnumSet(Type.INT8, Type.INT16);

    /**
     * Get the singleton instance.
     * @return SinkSchema
     */
    public static SinkSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return "tinyint";
    }

    @Override
    public Schema schemaOf(final String sinkDataTypeName) {
        return SchemaBuilder.int8()
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
    public Object convertToJavaTypeValue(final Schema source, final Object recordValue) {
        Number numberRecord = (Number) recordValue;
        return numberRecord.byteValue();
    }

    @Override
    public Object convertToDbTypeValue(final Schema source, final Object recordValue) {
        return recordValue;
    }

    @Override
    public String sinkDataTypeNameOf(final Schema schema) {
        return name();
    }
}
