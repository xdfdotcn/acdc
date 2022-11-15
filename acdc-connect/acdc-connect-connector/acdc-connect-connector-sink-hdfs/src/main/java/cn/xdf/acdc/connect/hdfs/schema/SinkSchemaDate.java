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

import cn.xdf.acdc.connect.hdfs.util.DateTimeUtils;
import java.util.Objects;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

public final class SinkSchemaDate implements SinkSchema {

    private static final SinkSchema INSTANCE = new SinkSchemaDate();

    private final Schema dateSchema = Date.SCHEMA;

    /**
     * Get the singleton instance.
     * @return SinkSchema
     */
    public static SinkSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return "date";
    }

    @Override
    public Schema schemaOf(final String sinkDataTypeName) {
        return Date.builder()
            .optional()
            .parameter(SinkSchemas.DATA_TYPE_NAME_KEY, sinkDataTypeName)
            .parameter(SinkSchemas.NAME_KEY, name())
            .build();
    }

    @Override
    public boolean isPromotable(final Type schema) {
        return schema == dateSchema.type();
    }

    @Override
    public boolean isCompatibility(final Schema source) {
        if (!Objects.equals(source.name(), dateSchema.name())) {
            return false;
        }
        return true;
    }

    @Override
    public Object convertToJavaTypeValue(final Schema source, final Object recordValue) {
        return DateTimeUtils.fixTimeZone((java.util.Date) recordValue);
    }

    @Override
    public Object convertToDbTypeValue(final Schema source, final Object recordValue) {
        java.util.Date date = (java.util.Date) recordValue;
        return new java.sql.Date(date.getTime());
    }

    @Override
    public String sinkDataTypeNameOf(final Schema schema) {
        return name();
    }
}
